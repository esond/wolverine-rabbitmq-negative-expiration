using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Wolverine;
using Wolverine.Postgresql;
using Wolverine.RabbitMQ;

const int trials = 10;
var scheduleIn = TimeSpan.FromSeconds(1);
var deliverWithin = TimeSpan.FromSeconds(4);

await using var rabbit = new RabbitMqBuilder("rabbitmq:4.3.1").Build();
await using var postgres = new PostgreSqlBuilder("postgres:17").Build();
await Task.WhenAll(rabbit.StartAsync(), postgres.StartAsync());

var builder = Host.CreateApplicationBuilder();
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o => o.SingleLine = true);
builder.Logging.SetMinimumLevel(LogLevel.Warning);

builder.UseWolverine(opts =>
{
    opts.PersistMessagesWithPostgresql(postgres.GetConnectionString());
    opts.Policies.UseDurableOutboxOnAllSendingEndpoints();
    opts.Policies.UseDurableInboxOnAllListeners();
    opts.Policies.DisableConventionalLocalRouting();

    opts.UseRabbitMq(new Uri(rabbit.GetConnectionString())).AutoProvision();
    opts.PublishMessage<Ping>().ToRabbitQueue("repro");
    opts.ListenToRabbitQueue("repro");
});

using var host = builder.Build();
await host.StartAsync();
var bus = host.Services.GetRequiredService<IMessageBus>();

Console.WriteLine();
Console.WriteLine($"Durable outbox and inbox on Postgres, RabbitMQ transport, ScheduledJobPollingTime at its default.");
Console.WriteLine($"Each trial schedules two messages for {scheduleIn.TotalSeconds:0}s from now, in the same call sequence:");
Console.WriteLine($"  timed-N      DeliverWithin = {deliverWithin.TotalSeconds:0}s, so it has {(deliverWithin - scheduleIn).TotalSeconds:0}s of slack after its scheduled time");
Console.WriteLine("  bystander-N  no expiry");
Console.WriteLine();

await bus.SendAsync(new Ping("control"));
await WaitFor(["control"], TimeSpan.FromSeconds(15));
Console.WriteLine($"control: {(PingHandler.Received.ContainsKey("control") ? "received" : "NOT received")}");
Console.WriteLine();

Console.WriteLine("trial  timed         bystander     broker 'invalid expiration' errors");
var errorsSoFar = 0;
var totals = (timedLost: 0, bystanderLost: 0, brokerErrors: 0);

for (var i = 1; i <= trials; i++)
{
    var timed = $"timed-{i}";
    var bystander = $"bystander-{i}";
    var scheduledTime = DateTimeOffset.UtcNow.Add(scheduleIn);

    await bus.SendAsync(new Ping(timed),
        new DeliveryOptions { ScheduledTime = scheduledTime, DeliverWithin = deliverWithin });
    await bus.SendAsync(new Ping(bystander), new DeliveryOptions { ScheduledTime = scheduledTime });

    await WaitFor([timed, bystander], TimeSpan.FromSeconds(12));

    var errors = await BrokerErrors();
    var newErrors = errors.Count - errorsSoFar;
    errorsSoFar = errors.Count;

    var timedReceived = PingHandler.Received.ContainsKey(timed);
    var bystanderReceived = PingHandler.Received.ContainsKey(bystander);
    totals = (totals.timedLost + (timedReceived ? 0 : 1),
        totals.bystanderLost + (bystanderReceived ? 0 : 1),
        totals.brokerErrors + newErrors);

    Console.WriteLine(
        $"{i,5}  {(timedReceived ? "received" : "NOT received"),-12}  {(bystanderReceived ? "received" : "NOT received"),-12}  {newErrors}");
}

Console.WriteLine();
Console.WriteLine("Results");
Console.WriteLine($"  timed messages not received:     {totals.timedLost} of {trials}");
Console.WriteLine($"  bystanders not received:         {totals.bystanderLost} of {trials}");
Console.WriteLine($"  RabbitMQ 'invalid expiration':   {totals.brokerErrors}");
foreach (var line in await BrokerErrors())
    Console.WriteLine($"    {line}");

Console.WriteLine();
Console.WriteLine("Expected (docs/guide/messaging/expiration.md: \"Wolverine will also discard outgoing messages that are");
Console.WriteLine("past their DeliverBy time\"): a timed message released after its DeliverBy is discarded, every");
Console.WriteLine("bystander is received, and the broker logs no errors.");

await host.StopAsync();

async Task<List<string>> BrokerErrors()
{
    var (stdout, stderr) = await rabbit.GetLogsAsync();
    return (stdout + stderr).Split('\n')
        .Where(l => l.Contains("invalid expiration"))
        .Select(l => Regex.Replace(l, @"\x1B\[[0-9;]*m", "").Trim())
        .ToList();
}

async Task WaitFor(string[] names, TimeSpan timeout)
{
    var deadline = DateTimeOffset.UtcNow.Add(timeout);
    while (DateTimeOffset.UtcNow < deadline && !names.All(PingHandler.Received.ContainsKey))
        await Task.Delay(250);
}

public record Ping(string Name);

public static class PingHandler
{
    public static readonly ConcurrentDictionary<string, DateTimeOffset> Received = new();

    public static void Handle(Ping ping) => Received.TryAdd(ping.Name, DateTimeOffset.UtcNow);
}
