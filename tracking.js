
'use strict';

const AWS = require('aws-sdk');
const mqtt = require('mqtt');
const sqs = new AWS.SQS({region : 'eu-west-1'});
const sns = new AWS.SNS({region : 'eu-west-1'});

function prefixWithZeroes(input, desiredLength) {
  const diff = Math.max(desiredLength - input.length, 0);
  const zeroes = new Array(diff).fill(0);
  return `${zeroes.join('')}${input}`;
}

module.exports.start = async (event) => {
  const data = JSON.parse(event.body);

  if (!data || !data.transport_mode || !data.operator_id || !data.vehicle_number) {
    console.error('Validation Failed');
    return {
      statusCode: 400,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Missing a required field',
    };
  }

  try {
    const res = await sns.publish({
      Message: JSON.stringify(data),
      TopicArn: process.env.SNS_ARN,
    }).promise();
    console.log(res);
    return {
      statusCode: 200,
      body: "Started tracking the vehicle"
    };
  } catch (e) {
    console.error(e);
    return {
      statusCode: 500,
      body: JSON.stringify(e)
    }
  }
}

module.exports.track = (event) => {
  return new Promise(async (resolve, reject) => {
    const worker = setTimeout(() => {
      resolve({
        statusCode: 200,
        body: 'Finished tracking',
      });
    }, 1000*60*10);

    const data = JSON.parse(event.Records[0].Sns.Message);
    const { 
      transport_mode, 
      operator_id, 
      vehicle_number 
    } = data;

    const targetQueue = `${transport_mode}${operator_id}${vehicle_number}`;

    const params = {
      QueueName: targetQueue,
      Attributes: {
        MessageRetentionPeriod: "120",
        VisibilityTimeout: "120"
      }
    }

    let QueueUrl = '';
    try {
      const response = await sqs.createQueue(params).promise();
      console.log(response);
      QueueUrl = response.QueueUrl;
    } catch (e) {
      console.error(e);
    }

    const topic = `/hfp/v1/journey/ongoing/${transport_mode}/${prefixWithZeroes(operator_id, 4)}/${prefixWithZeroes(vehicle_number, 5)}/+/+/+/+/+/+/#`;

    const client  = mqtt.connect('mqtts://mqtt.hsl.fi:8883');

    client.on('connect', function () {
      client.subscribe(topic);
      console.log('Connected');
    });

    client.on('message', async function (topic, message) {
      const vehicle_position = JSON.parse(message).VP;

      //Skip vehicles with invalid location
      if (!vehicle_position.lat || !vehicle_position.long) {
        return;
      }

      const route = vehicle_position.desi;
      //vehicles are identified with combination of operator id and vehicle id
      const vehicle = vehicle_position.oper + "/" + vehicle_position.veh;
      
      const position = vehicle_position.lat + "," + vehicle_position.long;
      const speed = vehicle_position.spd;

      console.log("Route "+route+" (vehicle "+vehicle+"): "+position+" - "+speed+"m/s");
    
      // write to sqs
      try {
        await sqs.sendMessage({
          QueueUrl,
          MessageBody: JSON.stringify({
            ...vehicle_position
          }),
        }).promise();
      } catch (e) {
        console.error(e);
      }
    });
  })
}

module.exports.getStatus = async (event) => {
  const { pathParameters={} } = event;
  const { transport_mode, operator_id, vehicle_number } = pathParameters;

  var params = {
    AttributeNames: [
       "SentTimestamp"
    ],
    MaxNumberOfMessages: 10,
    MessageAttributeNames: [
       "All"
    ],
    QueueUrl: `${process.env.SQS_URL}/${transport_mode}${operator_id}${vehicle_number}`,
    VisibilityTimeout: 20,
    WaitTimeSeconds: 0
   };
   const messages = await sqs.receiveMessage(params).promise();
  return {
    statusCode: 200,
    body: JSON.stringify(messages),
  };
}


// function getExecutionStatus(params){
//   return new Promise((res, rej) => {
//     stepfunctions.describeExecution(params, (err, { status }) => {
//       if (err) {
//         rej(err);
//       }
//       res(status);
//     })
//   })
// }

// function stopExecution(params){
//   return new Promise((res, rej) => {
//     stepfunctions.stopExecution(params, (err, data) => {
//       if (err) {
//         console.log(err, err.stack) // an error occurred
//         return rej(err)
//       }
//       console.log(data) // successful response
//       console.log(data.executionArn) // needed for cancels
//       console.log(data.startDate)
//       const response = {
//         statusCode: 200,
//         body: JSON.stringify({
//           message: 'Stopped the vehicle tracking',
//           params: params
//         }),
//       };
//       res(response);
//     })
//   })
// }

// function startExecution(params){
//   return new Promise((res, rej) => {
//     stepfunctions.startExecution(params, (err, data) => {
//       if (err) {
//         console.log(err, err.stack) // an error occurred
//         return rej(err)
//       }
//       console.log(data) // successful response
//       console.log(data.executionArn) // needed for cancels
//       console.log(data.startDate)
//       const response = {
//         statusCode: 200,
//         body: JSON.stringify({
//           message: 'Started the vehicle tracking',
//           params: params
//         }),
//       };
//       res(response);
//     })
//   })
// }

// module.exports.startStepFunction = async () => {
//   const params = {
//     name: 'taskName',
//     stateMachineArn: process.env.STATE_MACHINE_ARN,
//   }

//   const status = await getExecutionStatus(params);

//   if (status === 'RUNNING') {
//     await stopExecution(params);
//   }

//   const response = await startExecution(params);

//   return response;
// };
