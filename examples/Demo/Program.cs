using ReliableEndpoints;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddReliableEndpoints();
var app = builder.Build();
app.UseRouting();
app.UseReliableEndpoints();
app.MapPost("/orders", () => Results.Json(new { id = Guid.NewGuid() }))
    .WithReliability(new EndpointReliabilityOptions
    {
        KeyScope = context => context.User.Identity?.Name ?? "demo-user"
    });
app.Run();
