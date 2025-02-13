using eShopSupport.Backend.Api;
using eShopSupport.Backend.Data;
using eShopSupport.Backend.Services;
using eShopSupport.ServiceDefaults.Clients.PythonInference;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.SemanticKernel.Connectors.Qdrant;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel.Memory;
using SmartComponents.LocalEmbeddings.SemanticKernel;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire components.
builder.AddServiceDefaults();

builder.AddNpgsqlDbContext<AppDbContext>("backenddb");

// Qdrant vector db related service registration in DI container
builder.AddQdrantHttpClient("vector-db");
builder.Services.AddScoped(s => new QdrantMemoryStore(
    s.GetQdrantHttpClient("vector-db"), 384));

// register Qdrant vector database as memory store
builder.Services.AddScoped<IMemoryStore>(s => s.GetRequiredService<QdrantMemoryStore>());

// Local embeddings model by SmartComponents.LocalEmbeddings.SemanticKernel
// Embeddings are generated using this library. Instead you can use external AI models like (all-MiniLM-L6-v2) or OpenAi's embedding model
// Here we are adding a service in DI container. Which means you can access this service anywhere in the app by injecting the interface in constructor of the service (class) 
builder.Services.AddScoped<ITextEmbeddingGenerationService, LocalTextEmbeddingGenerationService>();

builder.Services.AddScoped<ISemanticTextMemory, SemanticTextMemory>();
builder.Services.AddScoped<ProductSemanticSearch>();
builder.Services.AddScoped<ProductManualSemanticSearch>();
builder.Services.AddScoped<TicketSummarizer>();
builder.Services.AddHttpClient<PythonInferenceClient>(c => c.BaseAddress = new Uri("http://python-inference"));
builder.AddAzureBlobClient("eshopsupport-blobs");

builder.AddChatCompletionService("chatcompletion");
builder.AddRedisClient("redis");

JsonWebTokenHandler.DefaultInboundClaimTypeMap.Remove("sub");

builder.Services.AddAuthentication().AddJwtBearer(options =>
{
    options.Authority = builder.Configuration["IdentityUrl"];
    options.TokenValidationParameters.ValidateAudience = false;
});
builder.Services.AddAuthorizationBuilder()
    .AddPolicy("CustomerApi", policy => policy.RequireAuthenticatedUser())
    .AddFallbackPolicy("StaffApi", policy => policy.RequireRole("staff"));

var app = builder.Build();

var initialImportDataDir = builder.Configuration["ImportInitialDataDir"];
await AppDbContext.EnsureDbCreatedAsync(app.Services, initialImportDataDir);
await ProductSemanticSearch.EnsureSeedDataImportedAsync(app.Services, initialImportDataDir);
await ProductManualSemanticSearch.EnsureSeedDataImportedAsync(app.Services, initialImportDataDir);

app.MapAssistantApiEndpoints();
app.MapTicketApiEndpoints();
app.MapTicketMessagingApiEndpoints();
app.MapCatalogApiEndpoints();

app.Run();
