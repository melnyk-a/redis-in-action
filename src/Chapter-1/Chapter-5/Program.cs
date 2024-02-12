// Counters
// hash where key is start of time slice and value is number of site hits
// sorted set for known, for clean up

using StackExchange.Redis;
using System.Xml.Linq;

ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

int[] precission = new int[]
{
    1,5,60,300,3600,1800,86400
};

async Task UpdateCounter(IDatabase db, string name, int count = 1)
{
    var now = NowInSeconds();
    var connection = db.CreateTransaction();
    foreach (var prec in precission)
    {
        var pnow = (now / prec) * prec; // get start of current time slice
        var hash = $"{prec}:{name}";
        await connection.SortedSetAddAsync("known:", hash, 0);
        await connection.HashIncrementAsync($"count:{hash}", pnow, count);
    }
    connection.Execute();
}

Dictionary<string, string> GetCounter(IDatabase db, string name, double precession)
{
    var hash = $"{precession}:{name}";
    var data = db.HashGetAll($"count:hash");
    var result = new Dictionary<string, string>();
    foreach (var key in data)
    {
        result.Add(key.Name.ToString(), key.Value.ToString());
    }
    result = result.OrderBy(x => x.Value).ToDictionary(obj => obj.Key, obj => obj.Value);
    return result;

}

void CleanCounters(IDatabase db)
{
    var passes = 0;
    bool quit = false;
    long timeOffset = 2 * 86400000;
    int sampleCount = 0;
    while (!quit)
    {
        var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeOffset;
        var index = 0;
        while (index < db.SortedSetLength("known:") && !quit)
        {
            var counterToCheck = db.SortedSetRangeByRank("known:", index, index);
            index++;
            if (counterToCheck.Length == 0)
            {
                break;
            }

            var hash = counterToCheck[0].ToString();

            var precision = int.Parse(hash.Substring(0, hash.IndexOf(':')));
            var bprec = precision / 60;
            if (bprec == 0)
            {
                bprec = 1;
            }

            if ((passes % bprec) != 0)
            {
                continue;
            }

            var counterHashKey = "count:" + hash;
            var cutoff = (((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeOffset) / 1000) - sampleCount * precision).ToString();
            var samples = new List<string>(db.HashKeys(counterHashKey).Select(c => c.ToString()));
            samples.Sort();
            var remove = bisectRight(samples, cutoff);

            if (remove != 0)
            {
                var samplesToRemove = samples.GetRange(0, remove).Select(c => new RedisValue(c)).ToArray();
                db.HashDelete(counterHashKey, samplesToRemove);
                var hashPrev = db.HashGetAll(counterHashKey);
                if (remove == samples.Count)
                {
                    var trans = db.CreateTransaction();
                    if (hashPrev.Length > 0)
                    {
                        foreach (var entry in hashPrev)
                        {
                            trans.AddCondition(Condition.HashEqual(counterHashKey, entry.Name, entry.Value));
                        }

                        trans.AddCondition(Condition.HashLengthEqual(counterHashKey, hashPrev.Length));
                    }
                    else
                    {
                        trans.AddCondition(Condition.HashLengthEqual(counterHashKey, 0));
                    }

                    if (hashPrev.Length == 0)
                    {
                        trans.SortedSetRemoveAsync("known:", hash);
                        trans.Execute();
                        index--;
                    }
                }
            }
        }
        passes++;
        var duration = Math.Min((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeOffset) - start + 1000, 60000);
        var timeToSleep = TimeSpan.FromMilliseconds(Math.Max(60000 - duration, 1000));
        Thread.Sleep(timeToSleep);
    }
}

// mimic python's bisect.bisect_right
int bisectRight(List<String> values, String key)
{
    var index = values.BinarySearch(key);
    return index < 0 ? Math.Abs(index) - 1 : index + 1;
}
double NowInSeconds()
{
    return TimeSpan.FromTicks(DateTime.UtcNow.Ticks).TotalSeconds;
}