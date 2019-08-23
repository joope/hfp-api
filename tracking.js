
'use strict';

const AWS = require('aws-sdk');
const mqtt = require('mqtt');
const dynamoDb = new AWS.DynamoDB.DocumentClient();
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
    await sns.publish({
      Message: JSON.stringify(data),
      TopicArn: process.env.SNS_ARN,
    }).promise();
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
    setTimeout(() => {
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

      const params = {
        TableName: process.env.DYNAMODB_TABLE,
        Item: {
          lat: vehicle_position.lat,
          long: vehicle_position.long,
          vehicleId: `${transport_mode}${operator_id}${vehicle_number}`,
          createdAt: new Date().getTime(),
        },
      };

      try {
        dynamoDb.put(params).promise()
      } catch (e) {
        console.error(e);
      }
    });
  })
}

module.exports.getStatus = async (event) => {
  const { pathParameters={} } = event;
  const { transport_mode, operator_id, vehicle_number } = pathParameters;

  try {
    const params = {
      TableName: process.env.DYNAMODB_TABLE,
      KeyConditionExpression: "vehicleId = :vehicle and createdAt > :date",
      ExpressionAttributeValues: {
        ":vehicle": `${transport_mode}${operator_id}${vehicle_number}`,
        ":date": new Date().getTime() - 1000*120
      }, 
      Select: "ALL_ATTRIBUTES",
    };
    const res = await dynamoDb.query(params).promise();
    const polyline = res.Items.map(item => ([item.lat, item.long]));
    const latest = res.Items.pop();
    const currentPosition = [latest.lat, latest.long]

    return {
      statusCode: 200,
      body: JSON.stringify({
        currentPosition,
        polyline
      }),
    };
  } catch (e) {
    return {
      statusCode: 500,
      body: JSON.stringify(e),
    }
  }
}