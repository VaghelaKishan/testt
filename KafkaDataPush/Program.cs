using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {

        Console.WriteLine("Enter Kafka broker endpoint:");
        string brokerList = "192.168.3.147:9092";

        Console.WriteLine("Enter Kafka topic to produce to:");
        string topic = "EJGALLO_SLS_PICO_DELETED_OPPORTUNITY";

        Console.WriteLine("Enter number of records to produce:");
        int recordCount = int.Parse(Console.ReadLine());

        Console.WriteLine("Enter batch size:");
        int batchSize = int.Parse(Console.ReadLine());

        // JSON data to produce
        //string jsonData = "{\"City\":\"Kishan\",\"State\":\"GAaaa\",\"Banner\":\"\",\"County\":\"Floyd\",\"Channel\":\"Dining\",\"Country\":\"USA\",\"ZipCode\":\"30165\",\"Address1\":\"1803 Shorter Ave SW\",\"Address2\":\"\",\"Latitude\":\"34.26910000\",\"Longitude\":\"-85.21580000\",\"SalesType\":\"ON\",\"KeyAcctDBA\":\"Independent Dining - Casual\",\"RegionCode\":\"3534\",\"RegionName\":\"Eastern Area\",\"AccountName\":\"Gondola Pizza\",\"AccountTags\":\"Zone 5\",\"BeerCluster\":null,\"KeyAcctCorp\":\"Independent On-Sale\",\"LicenseType\":\"WINE/LIQUOR\",\"WineCluster\":\"\",\"AccountStatus\":\"Active\",\"LiquorCluster\":null,\"ClientAccountId\":\"123029\",\"KeyAcctDivision\":\"Independent Dining - Casual\",\"DistributorAccounts\":[{\"shipTo\":\"10017877\",\"soldTo\":\"10017876\",\"rtlAcctId\":\"42583353\",\"shipToDesc\":\"Atlanta-National Dist\",\"soldToDesc\":\"NATIONAL DISTRIBUTING COMPANY INC\",\"distAcctNbr\":\"121871\"}],\"NonAlcoholicCluster\":null}";
        
        string jsonData = "{\"ClientAccountId\":\"1141295\",\"AccountName\":\"Hammerhead Grille\",\"Address1\":\"1230 Ocean Rd\",\"Address2\":\"\",\"City\":\"Narragansett\",\"County\":\"WASHINGTON\",\"State\":\"RI\",\"Country\":\"USA\",\"ZipCode\":\"02882\",\"Latitude\":\"41.37306501\",\"Longitude\":\"-71.48468508\",\"SalesType\":\"ON\",\"Channel\":\"Dining\",\"Banner\":\"\",\"LicenseType\":\"ALL\",\"KeyAcctCorp\":\"Independent On-Sale\",\"KeyAcctDBA\":\"Independent Dining - Casual\",\"KeyAcctDivision\":\"Independent Dining - Casual\",\"WineCluster\":\"\",\"LiquorCluster\":null,\"BeerCluster\":null,\"NonAlcoholicCluster\":null,\"AccountStatus\":\"OutOfBusiness\",\"AccountTags\":\"Zone 5\",\"RegionCode\":\"3538\",\"RegionName\":\"Mega Area\",\"DistributorAccounts\":[]}";
        //string jsonData = "{\"ClientAccountId\":531442,\"FileName\":\"WINE-WINE-Kroger_898_D01_C047_V151_F004_MX_5807519_4842432\",\"DatePlanogramLoaded\":\"2023-04-19 08:04:06\",\"Description\":\"Schematic\",\"ContenturlC\":\"https://s3-us-west-2.amazonaws.com/digital-distributor-docs/WINE-Kroger_898_D01_C047_V151_F004_MX_5807519_4842432.pdf\",\"PlanogramStatus\":\"A\"}";

        //string jsonData = "{\"ClientAccountId\":43437296,\"LastMonthPhyCases\":\"0\",\"LastYearLastMonthPhyCases\":\"0\",\"Closed4moPhyCases\":\"0\",\"LastYearClosed4moPhyCases\":\"0\",\"YearToDatePhyCases\":\"0\",\"LastYearToDatePhyCases\":\"0\",\"Closed12moPhyCases\":\"0\",\"LastYearClosed12moPhyCases\":\"1\",\"LastModifiedDate\":\"2023-09-12 07:52:32.361005\"}";

        // Create producer configuration
        var config = new ProducerConfig
        {
            BootstrapServers = brokerList
        };

        // Create producer
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                // Produce messages in batches
                for (int i = 0; i < recordCount; i += batchSize)
                {
                    var batch = GenerateBatch(jsonData, batchSize);
                    await ProduceBatchAsync(producer, topic, batch);
                    Console.WriteLine($"Produced {Math.Min(batchSize, recordCount - i)} records.");
                }

                Console.WriteLine($"All {recordCount} records have been produced successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}");
            }
        }
    }

    static IEnumerable<string> GenerateBatch(string jsonData, int batchSize)
    {
        for (int i = 0; i < batchSize; i++)
        {
            yield return jsonData;
        }
    }

    static async Task ProduceBatchAsync(IProducer<Null, string> producer, string topic, IEnumerable<string> batch)
    {
        var tasks = new List<Task>();

        foreach (var message in batch)
        {
            var task = producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
    }
}

