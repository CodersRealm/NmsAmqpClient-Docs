# CodersRealm.NmsAmqpClient

A simple, modern .NET wrapper for Apache NMS AMQP.This library simplifies working with message queues and topics by providing a single, unified client for any AMQP-compatible message broker.

## ✨ Key Features

✅ **Simple API** - Clean, modern .NET API with async/await  
✅ **Dependency Injection** - Full DI support with hosted service lifecycle  
✅ **Multiple Brokers** - Works with RabbitMQ, Azure Service Bus, ActiveMQ, Artemis  
✅ **Patterns** - Send/Receive, Pub/Sub, Request/Response  
✅ **Reliability** - Auto-reconnection, acknowledgments, transactions  
✅ **Type-Safe** - Strongly-typed message handling

## Install the Package

```bash
dotnet add package CodersRealm.NmsAmqpClient
```

## 🚀 Quick Start Guide 

### Worker Service Example

#### 1. Register Services
In your Worker Service `Program.cs` file, add your configuration and register the library dependencies into the Service Collection:

```csharp
using CodersRealm.NmsAmqpClient;

var builder = Host.CreateApplicationBuilder(args);

// Quick connection example
builder.Services.AddNmsAmqpClient("amqp://localhost:5672?nms.username=admin&nms.password=admin");

builder.Services.AddHostedService<Worker>();
var host = builder.Build();
host.Run();
```

#### 2. Inject and Use
Inject the library interface into your `Worker.cs` background service constructor to use its features:

```csharp
using CodersRealm.NmsAmqpClient;

public class Worker(ILogger<Worker> logger, IClient client) : BackgroundService
{
	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		// The client starts automatically upon instantiation; no explicit start call is required.

		// Create producer
		var producer = await client.Producers.CreateAsync("my-queue");

		// Send message
		var message = await producer.CreateTextMessageAsync("Hello World!");
		await producer.SendMessageAsync(message);

		// Create consumer
		var consumerOptions = new ConsumerOptions
		(
			QueueName: "my-queue",
			AcknowledgementMode: AcknowledgementMode.AutoAcknowledge,
			OnMessageReceivedAsync: async (e, token) =>
			{
				if (e.Message is ITextMessage textMsg)
				{
					logger.LogInformation("Received: {Text}", textMsg.Text);
					await ProcessMessageAsync(textMsg); // best to return fast
				}
			}
		);

		var consumer = await client.Consumers.CreateAsync(consumerOptions);

		await Task.Delay(Timeout.Infinite, stoppingToken);
	}

	// The client automatically shuts down and disposes safely with the host lifecycle.
}
```

### Console Example

```csharp
using CodersRealm.NmsAmqpClient;

string conn = "amqps://myServiceBusNamespace.servicebus.windows.net?nms.username=mySasPolicyName&nms.password=myUrlEncocdedSasKeyValue";

await using var client = new Client(conn);
await client.StartAsync();

// producer or consumer or subscriber code here

```

### Producer - Send a Message
```csharp
var producer = await client.Producers.CreateAsync("orders-queue");
var message = await producer.CreateTextMessageAsync("Order #123");
await producer.SendMessageAsync(message);
```

### Simple Consumer
```csharp
var options = new ConsumerOptions
(
	QueueName: "orders-queue",
	OnMessageReceivedAsync: async (e, token) =>
	{
		var text = (e.Message as ITextMessage)?.Text;
		logger.LogInformation("Order: {Text}", text);
	}
);
var consumer = await client.Consumers.CreateAsync(options);

await Task.Delay(Timeout.Infinite, stoppingToken);

```

### Simple Subscriber 
```csharp
// Publish
var producer = await client.Producers.CreateAsync("events-topic");
var message = await producer.CreateTextMessageAsync("Event occurred!");
await producer.SendMessageAsync(message);

// Subscribe
var options = new SubscriberOptions
(
	TopicName: "events-topic",
	SubscriptionName: "worker-sub",
	OnMessageReceivedAsync: async (e, token) =>
	{
		logger.LogInformation("Event: {Msg}", (e.Message as ITextMessage)?.Text);
		e.Action = MessageAction.Ack;
	}
);
var subscriber = await client.Subscribers.CreateAsync(options);

await Task.Delay(Timeout.Infinite, stoppingToken);

```

### Request-Response Pattern 

#### 1. The Requester Side

```csharp

// Options for creating the producer that will be sending the request messages
var producerOptions = new ProducerOptions 
{ 
	QueueName = "request-queue" // where the producer sends the request messages
};

// Options for creating the consumer portion of the Requester that will be processing the response messages
var responseMessageConsumerOptions = new ConsumerOptions
{
	QueueName = "response-queue", // The queue for the response messages
	MessageHandler = async (messageQueueEventArgs, token) =>
	{
		if (messageQueueEventArgs.Message is ITextMessage textMessage)
		{
			logger.LogInformation("Response: {Text}", textMessage.Text);
		}
	}
};

// Create the Requester (producer + consumer)
var requester = await client.Producers.CreateAsync(producerOptions, responseMessageConsumerOptions);

// Send request
var requestMsg = await requester.CreateTextMessageAsync("Get User Details");
requestMsg.NMSCorrelationID = Guid.NewGuid().ToString();
requestMsg.NMSReplyTo = await requester.GetQueueAsync("response-queue"); // tell responder where to send the response message.
await requester.SendMessageAsync(requestMsg);
```

#### 2. The Responder Side

```csharp
// Options for creating the consumer that will be processing the request messages
var consumerOptions = new ConsumerOptions
{
	QueueName = "request-queue",
	MessageHandler = async (messageQueueEventArgs, token) =>
	{
		// Process the request

		if (messageQueueEventArgs.Message is ITextMessage requestMessage)
		{
			logger.LogInformation("Request Message: {Text}", requestMessage.Text);

			// Modify the message payload to return it as a response
			requestMessage.ClearBody();
			requestMessage.Text = "The Response Message That Has The User Details";
			messageQueueEventArgs.ResponseMessage = requestMessage;
		}
	}
};

// Options for creating the producer that will be sending the response messages
var responderOptions = new ResponderOptions
{
	ResponseDestinationType = ResponseDestinationType.Queue,
	ResponseQueueName = "response-queue"
};

// Create the responder (consumer + producer)
var responder = await client.Consumers.CreateAsync(consumerOptions, responderOptions);

```

## 🌐 Broker Connection Strings

### ActiveMQ, Artemis, RabbitMQ
```
// Standard connection
amqp[s]://{Host}:{Port}?nms.username={UserName}&nms.password={Password}

// Example:
amqps://localhost:5672?nms.username=admin&nms.password=admin

// Failover and automatic reconnection
failover:(amqp[s]://{Host}:{Port}[, amqp[s]://{Host}:{Port}, ...])?nms.username={UserName}&nms.password={Password}

//Example:
failover:(amqps://host1:5672,amqps://host2:5672)?nms.username=admin&nms.password=admin

```

### Azure Service Bus with Shared Access Key
```
amqps://{Namespace}.servicebus.windows.net?nms.username={SasPolicyName}&nms.password={UrlEncodedSasKeyValue}
```

## 🔧 Configuration Options

### Basic Setup
```csharp
builder.Services.AddNmsAmqpClient("amqp://localhost:5672");
```

### Advanced Setup
```csharp
builder.Services.AddNmsAmqpClient(options =>
{
	options.ConnectionString = builder.Configuration.GetConnectionString("MessageBroker")!;
	options.RedeliveryPolicy = new DefaultRedeliveryPolicy
	{
		MaxRedeliveries = 5,
		InitialRedeliveryDelay = 1000
	};
});
```


## 🆘 Troubleshooting

### Connection Failed
- Verify the target broker instance is actively running.
- Ensure host, port, and credential values match your environment.
- Confirm network firewall rules allow traffic through target AMQP ports (usually 5672 / 5671).

### Messages Not Received
- Confirm the initialization sequence completes without exceptions.
- Verify queue and topic naming conventions match case sensitivity layouts.
- Validate that your client's designated AcknowledgementMode aligns with broker rules.

### Performance Issues
- Scale up the `PrefetchCount` configuration setting to maximize network pipelines. See **[Configurations](https://github.com/apache/activemq-nms-amqp/blob/main/docs/configuration.md)**.
- Switch to `DupsOkAcknowledge` mode if system logic permits duplicate processing.
- Provision parallel consumer instances to distribute concurrent processing loads.

## 🤝 Support and Feedback
- **[Issues](https://github.com/CodersRealm/NmsAmqpClient-Docs/issues)**: Report bugs or request features
- **[Discussions](https://github.com/CodersRealm/NmsAmqpClient-Docs/discussions)**: Ask questions and share ideas
- **[API](https://codersrealm.github.io/NmsAmqpClient-Docs/api/CodersRealm.NmsAmqpClient.html)**: View the API documentation
- **[Examples](https://github.com/CodersRealm/NmsAmqpClient-Docs)**: View the examples for more patterns


## 📄 License & Usage Tiers

Copyright © 2026 CodersRealm.com. All rights reserved.

This package is governed by the **CodersRealm End User License Agreement (EULA)**. By installing or using this software, you agree to its terms.

### 🆓 Free Tier Authorization
You are granted a limited, non-exclusive, revocable right to use and redistribute the compiled binaries (`.dll`) **only when embedded inside your own software applications** for:
*   **Personal & Educational** - Non-commercial learning, research, or hobby projects.
*   **Development & Testing** - Internal testing, evaluation, and staging environments.
*   **Small-Scale Production** - Commercial applications where your individual or corporate entity generates **less than \$10,000 USD** in gross annual revenue.

### 🚫 Core Restrictions
*   **No Standalone Redistribution** - You cannot resell, lease, or distribute this package as a raw development component or competing library.
*   **No Reverse Engineering** - Decompiling, disassembling, or attempting to derive the source code is strictly prohibited.
*   **Retain Proprietary Notices** - You must not remove or alter any copyright or brand identifiers embedded in the software.

### 💼 Commercial License Requirement
If your corporate entity generates **\$10,000 USD or more** in gross annual revenue, or if you require dedicated technical support, enterprise deployment rights, or formal corporate procurement agreements, your Free Tier authorization automatically terminates. Your continued use requires a Commercial License.

*   📧 **Get a Commercial License**: Contact [sales@codersrealm.com](mailto:sales@codersrealm.com) for upgrade and enterprise pricing options.

*Disclaimer: The software is provided "AS IS", without warranty of any kind. Licensor shall not be liable for any damages arising out of the use or inability to use this product.*

**Happy Messaging!** 🚀

