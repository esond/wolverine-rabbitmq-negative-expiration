using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.RabbitMq;
using Wolverine;
using Wolverine.RabbitMQ;

await using var rabbit = new RabbitMqBuilder("rabbitmq:4.3.1").Build();
await rabbit.StartAsync();

var builder = Host.CreateApplicationBuilder();
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o => o.SingleLine = true);
builder.Logging.SetMinimumLevel(LogLevel.Warning);

builder.UseWolverine(opts =>
{
    opts.Policies.DisableConventionalLocalRouting();
    opts.UseRabbitMq(new Uri(rabbit.GetConnectionString())).AutoProvision();
    opts.PublishMessage<Ping>().ToRabbitQueue("repro");
    opts.ListenToRabbitQueue("repro");
});

using var host = builder.Build();
await host.StartAsync();
var bus = host.Services.GetRequiredService<IMessageBus>();

Console.WriteLine();
Console.WriteLine("Control: send a normal message before anything expires.");
await bus.SendAsync(new Ping("control"));
await WaitFor("control");

Console.WriteLine();
Console.WriteLine("Scenario 1: send a message whose DeliverBy has passed, then a normal message.");
await bus.SendAsync(new Ping("expired-send"), new DeliveryOptions { DeliverBy = DateTimeOffset.UtcNow.AddSeconds(-1) });
await bus.SendAsync(new Ping("bystander-1"));
await WaitFor("bystander-1");

Console.WriteLine();
Console.WriteLine("Scenario 2: schedule a message for 2s from now that expires 1s from now, plus a normal one at 2s.");
var now = DateTimeOffset.UtcNow;
await bus.SendAsync(new Ping("expired-scheduled"),
    new DeliveryOptions { ScheduledTime = now.AddSeconds(2), DeliverBy = now.AddSeconds(1) });
await bus.SendAsync(new Ping("bystander-2"), new DeliveryOptions { ScheduledTime = now.AddSeconds(2) });
await WaitFor("bystander-2");

var (stdout, stderr) = await rabbit.GetLogsAsync();
var brokerErrors = (stdout + stderr).Split('\n')
    .Where(l => l.Contains("invalid expiration"))
    .Select(l => Regex.Replace(l, @"\x1B\[[0-9;]*m", ""))
    .ToList();

Console.WriteLine();
Console.WriteLine("Results");
Console.WriteLine($"  Received by the listener: [{string.Join(", ", PingHandler.Received)}]");
Console.WriteLine($"  RabbitMQ 'invalid expiration' errors: {brokerErrors.Count}");
foreach (var line in brokerErrors)
    Console.WriteLine($"    {line.Trim()}");

Console.WriteLine();
Console.WriteLine("Expected (docs/guide/messaging/expiration.md: \"Wolverine will also discard outgoing messages that are");
Console.WriteLine("past their DeliverBy time\"): both expired messages discarded, the control and both bystanders received,");
Console.WriteLine("no broker errors.");

await host.StopAsync();

async Task WaitFor(string name)
{
    var deadline = DateTimeOffset.UtcNow.AddSeconds(15);
    while (DateTimeOffset.UtcNow < deadline && !PingHandler.Received.Contains(name))
        await Task.Delay(250);

    Console.WriteLine(PingHandler.Received.Contains(name)
        ? $"  {name} was received."
        : $"  {name} was NOT received within 15s.");
}

public record Ping(string Name);

public static class PingHandler
{
    public static readonly ConcurrentBag<string> Received = [];

    public static void Handle(Ping ping) => Received.Add(ping.Name);
}
