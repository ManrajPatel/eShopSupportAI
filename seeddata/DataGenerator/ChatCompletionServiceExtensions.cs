using Microsoft.Extensions.DependencyInjection;
using System.Data.Common;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.AI;
using Azure.AI.OpenAI;
using System.ClientModel;
using OpenAI;

namespace eShopSupport.DataGenerator;

public static class ChatCompletionServiceExtensions
{
    public static void AddOpenAIChatCompletion(this HostApplicationBuilder builder, string connectionStringName)
    {
        var connectionStringBuilder = new DbConnectionStringBuilder();
        var connectionString = builder.Configuration.GetConnectionString(connectionStringName);
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException($"Missing connection string {connectionStringName}");
        }

        connectionStringBuilder.ConnectionString = connectionString;

        var deployment = connectionStringBuilder.TryGetValue("Deployment", out var deploymentValue) ? (string)deploymentValue : throw new InvalidOperationException($"Connection string {connectionStringName} is missing 'Deployment'");
        var endpoint = connectionStringBuilder.TryGetValue("Endpoint", out var endpointValue) ? (string)endpointValue : throw new InvalidOperationException($"Connection string {connectionStringName} is missing 'Endpoint'");
        var key = connectionStringBuilder.TryGetValue("Key", out var keyValue) ? (string)keyValue : throw new InvalidOperationException($"Connection string {connectionStringName} is missing 'Key'");

        builder.Services.AddSingleton<OpenAIClient>(_ => new AzureOpenAIClient(
            new Uri(endpoint), new ApiKeyCredential(key)));

        builder.Services.AddChatClient(builder => builder.GetRequiredService<OpenAIClient>().AsChatClient(deployment))
            .UseFunctionInvocation();
    }

    public static void AddOllamaChatCompletion(this HostApplicationBuilder builder, string serviceName)
    {
        var configKey = $"{serviceName}:LlmModelName";
        var modelName = builder.Configuration[configKey];

        if (string.IsNullOrEmpty(modelName))
        {
            throw new InvalidOperationException($"No {nameof(modelName)} was specified, and none could be found from configuration at '{configKey}'");
        }

        var uri = new Uri("http://localhost:11434");

        ChatClientBuilder chatClientBuilder = builder.Services.AddChatClient(serviceProvider => {
            var httpClient = serviceProvider.GetService<HttpClient>() ?? new();
            // httpClient.Timeout = TimeSpan.FromMinutes(2);
            return new OllamaChatClient(uri, modelName, httpClient);
        });

        // Temporary workaround for Ollama issues
        chatClientBuilder.UsePreventStreamingWithFunctions();
    }
}
