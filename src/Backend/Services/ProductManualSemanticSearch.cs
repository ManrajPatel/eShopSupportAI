using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Text.Json;
using Azure.Storage.Blobs;
using eShopSupport.Backend.Data;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel.Memory;

namespace eShopSupport.Backend.Services;

public class ProductManualSemanticSearch(ITextEmbeddingGenerationService embedder, IServiceProvider services)
{
    private const string ManualCollectionName = "manuals";

    public async Task<IReadOnlyList<MemoryQueryResult>> SearchAsync(int? productId, string query)
    {
        // embeddings (vector representation) for search query
        var embedding = await embedder.GenerateEmbeddingAsync(query);
        // If productId is provided, it filters the search results to only include manuals for that product.
        // This filtering is applied inside Qdrant
        var filter = !productId.HasValue
            ? null
            : new
            {
                must = new[]
                {
                    new { key = "external_source_name", match = new { value = $"productid:{productId}" } }
                }
            };

        // HTTP client to interact with the Qdrant vector database
        var httpClient = services.GetQdrantHttpClient("vector-db");

        // Search request to Qdrant API. Search will be done in ManualCollectionName collection
        var response = await httpClient.PostAsync($"collections/{ManualCollectionName}/points/search",
            JsonContent.Create(new
            {
                vector = embedding,
                // Specifies which fields should be returned (id, text, etc.).
                with_payload = new[] { "id", "text", "external_source_name", "additional_metadata" },
                limit = 3, // Limits results to 3 best matches
                filter, // filter for product id
            }));

        // The response from Qdrant is parsed into object (QdrantResult)
        var responseParsed = await response.Content.ReadFromJsonAsync<QdrantResult>();

        // Each Qdrant search result is converted into a MemoryQueryResult
        return responseParsed!.Result.Select(r => new MemoryQueryResult(
            new MemoryRecordMetadata(true, r.Payload.Id, r.Payload.Text, "", r.Payload.External_Source_Name, r.Payload.Additional_Metadata),
            r.Score, // similarity score (higher = more relevant).
            null)).ToList();
    }

    public static async Task EnsureSeedDataImportedAsync(IServiceProvider services, string? initialImportDataDir)
    {
        if (!string.IsNullOrEmpty(initialImportDataDir))
        {
            using var scope = services.CreateScope();
            await ImportManualFilesSeedDataAsync(initialImportDataDir, scope);
            await ImportManualChunkSeedDataAsync(initialImportDataDir, scope);
        }
    }

    /// <summary>
    /// Seeds manual files (.pdfs) from manuals.zip to Azure Blob Storage container named "manuals"
    /// </summary>
    /// <param name="importDataFromDir">source folder path where the manuals.zip resides</param>
    /// <param name="scope">Service scope</param>
    /// <returns></returns>
    private static async Task ImportManualFilesSeedDataAsync(string importDataFromDir, IServiceScope scope)
    {
        var blobStorage = scope.ServiceProvider.GetRequiredService<BlobServiceClient>();
        var blobClient = blobStorage.GetBlobContainerClient("manuals");
        if (await blobClient.ExistsAsync())
        {
            return;
        }

        await blobClient.CreateIfNotExistsAsync();

        var manualsZipFilePath = Path.Combine(importDataFromDir, "manuals.zip");
        using var zipFile = ZipFile.OpenRead(manualsZipFilePath);
        foreach (var file in zipFile.Entries)
        {
            using var fileStream = file.Open();
            await blobClient.UploadBlobAsync(file.FullName, fileStream);
        }
    }

    /// <summary>
    /// Insert manual embeddings (chunk data) into Qdrant vector database
    /// </summary>
    /// <param name="importDataFromDir"></param>
    /// <param name="scope"></param>
    /// <returns></returns>
    private static async Task ImportManualChunkSeedDataAsync(string importDataFromDir, IServiceScope scope)
    {
        // Retrieves an IMemoryStore instance from the DI (Dependency Injection) container
        // IMemoryStore is an abstraction for vector database Qdrant, please refer program.cs of this project for related registration code
        var semanticMemory = scope.ServiceProvider.GetRequiredService<IMemoryStore>();
        var collections = await semanticMemory.GetCollectionsAsync().ToListAsync(); // fetch existing collections

        // if collection not exists
        if (!collections.Contains(ManualCollectionName))
        {
            await semanticMemory.CreateCollectionAsync(ManualCollectionName); // create 'manuals' collection

            using var fileStream = File.OpenRead(Path.Combine(importDataFromDir, "manual-chunks.json"));
            var manualChunks = JsonSerializer.DeserializeAsyncEnumerable<ManualChunk>(fileStream); // convert json to ManualChunk object list
            
            // asynchronously receive chunk of 1000 ManualChunk objects
            await foreach (var chunkChunk in ReadChunkedAsync(manualChunks, 1000))
            {
                // Generate mappedRecords for each chunk of 1000 records
                var mappedRecords = chunkChunk.Select(chunk =>
                {
                    var id = chunk!.ChunkId.ToString();
                    var metadata = new MemoryRecordMetadata(false, id, chunk.Text, "", $"productid:{chunk.ProductId}", $"pagenumber:{chunk.PageNumber}");
                    var embedding = MemoryMarshal.Cast<byte, float>(new ReadOnlySpan<byte>(chunk.Embedding)).ToArray();
                    return new MemoryRecord(metadata, embedding, null);
                });

                // Insert/Update 1000 records at once in Qdrant vector Db
                await foreach (var _ in semanticMemory.UpsertBatchAsync(ManualCollectionName, mappedRecords)) { }
            }
        }
    }

    private static async Task<bool> HasAnyAsync<T>(IAsyncEnumerable<T> asyncEnumerable)
    {
        await foreach (var item in asyncEnumerable)
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// For provided chunkLength (1000), it will return list of T (ManualChunk)
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="source"></param>
    /// <param name="chunkLength"></param>
    /// <returns></returns>
    private static async IAsyncEnumerable<IEnumerable<T>> ReadChunkedAsync<T>(IAsyncEnumerable<T> source, int chunkLength)
    {
        var buffer = new T[chunkLength];
        var index = 0;
        await foreach (var item in source)
        {
            buffer[index++] = item;
            if (index == chunkLength)
            {
                // This will return the list of 1000 ManualChunk objects to the calling function and will resume to build next chunk of 1000
                yield return new ArraySegment<T>(buffer, 0, index);
                index = 0;
            }
        }

        if (index > 0)
        {
            yield return new ArraySegment<T>(buffer, 0, index);
        }
    }

    class QdrantResult
    {
        public required QdrantResultEntry[] Result { get; set; }
    }

    class QdrantResultEntry
    {
        public float Score { get; set; }
        public required QdrantResultEntryPayload Payload { get; set; }
    }

    class QdrantResultEntryPayload
    {
        public required string Id { get; set; }
        public required string Text { get; set; }
        public required string External_Source_Name { get; set; }
        public required string Additional_Metadata { get; set; }
    }
}
