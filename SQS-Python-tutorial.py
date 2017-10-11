import boto3
from botocore.exceptions import ClientError

sqs = boto3.resource('sqs')

try:
	queue_created = sqs.create_queue(QueueName='TestQueue', Attributes={'DelaySeconds': '3'})
	print(queue_created.url)
	print(queue_created.attributes.get('DelaySeconds'))
except ClientError as Ex:
	if Ex.response['Error']['Code'] == 'QueueAlreadyExists':
		print "Queue already exists"
	else:
		print "UnKnown Exception"


# Get the queue
queue = sqs.get_queue_by_name(QueueName='TestQueue')

# Print out each queue name, which is part of its ARN
for queue in sqs.queues.all():
    print(queue.url)

# Create a new message
response = queue.send_message(MessageBody='Testing SQS using boto3 library')

# The response is NOT a resource, but gives you a message ID and MD5
print(response.get('MessageId'))
print(response.get('MD5OfMessageBody'))


# creating messages with custom attributes:
queue.send_message(MessageBody='boto3', MessageAttributes={
    'Author': {
        'StringValue': 'Kartheek',
        'DataType': 'String'
    }
})

# Sending Messages in batches
response = queue.send_messages(Entries=[
    {
        'Id': '1',
        'MessageBody': 'world'
    },
    {
        'Id': '2',
        'MessageBody': 'boto3',
        'MessageAttributes': {
            'Author': {
                'StringValue': 'Daniel',
                'DataType': 'String'
            }
        }
    }
])

# Print out any success or failure
if response.get('Failed') is None:
	print('Success')
else:
	print(response.get('Failed'))


# Process messages by printing out body and optional author name
for message in queue.receive_messages(MessageAttributeNames=['Author']):
    # Get the custom author message attribute if it was set
    author_text = ''
    if message.message_attributes is not None:
        author_name = message.message_attributes.get('Author').get('StringValue')
        if author_name:
            author_text = ' ({0})'.format(author_name)

    # Print out the body and author (if set)
    print('Hello, {0}!{1}'.format(message.body, author_text))

    # Let the queue know that the message is processed
    message.delete()


response = queue.delete()

print(response["ResponseMetadata"]["HTTPStatusCode"])