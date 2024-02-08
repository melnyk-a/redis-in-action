using StackExchange.Redis;

ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

//for (int i = 0; i < 50; i++)
//{
//    UpdateToken(db, $"token {i}", $"user {i}", $"product {i}");
//}
CleanSession(db);

RedisValue CheckToken(IDatabase db, string token)
{
    return db.HashGet("login:", token);
}

void UpdateToken(IDatabase db, string token, string user, string? itemId)
{
    var now = NowInSeconds();
    db.HashSet("login:", token, user);
    db.SortedSetAdd("recent:", token, now);

    if (itemId != null)
    {
        var key = $"viewed:{token}";
        db.SortedSetAdd(key, itemId, now);
        db.SortedSetRemoveRangeByRank(key, 0, -26);
        db.SortedSetDecrement($"viewed:", itemId, 1);
    }
}

async Task CleanSession(IDatabase db)
{
    var quit = false;
    const int LIMIT = 5;

    while (!quit)
    {
        var size = db.SortedSetLength("recent:");
        if (size < LIMIT)
        {
            await Task.Delay(TimeSpan.FromSeconds(1));
            continue;
        }

        var end = long.Min(size - LIMIT, 100);
        var tokens = db.SortedSetRangeByRank("recent:", 0, end - 1);

        var sessionKeys = new List<RedisKey>();
        foreach (var token in tokens)
        {
            sessionKeys.Add($"viewed:{token}");
            sessionKeys.Add($"cart:{token}");
        }

        db.KeyDelete([.. sessionKeys]);
        db.HashDelete("login:", tokens);
        db.SortedSetRemove("recent:", tokens);
    }
}

void AddToCard(IDatabase db, string session, string item, int count)
{
    var key = $"cart:{session}";
    if (count <= 0)
    {
        db.HashDelete(key, item);
    }
    else
    {
        db.HashSet(key, item, count);
    }
}


string CasheRequest(IDatabase db, string request, Func<string, string> callback)
{
    if (!CanCache(db, request))
    {
        return callback(request);
    }

    var pageKey = $"cache:{HashRequest(request)}";
    var content = db.StringGet(pageKey);

    if (content == RedisValue.Null)
    {
        content = callback(request);
        db.StringSet(pageKey, content, TimeSpan.FromSeconds(300));
    }
    return content!;
}

string HashRequest(string request)
{
    throw new NotImplementedException();
}

bool CanCache(IDatabase db, string request)
{
    var itemId = GetItemId(request);
    if (itemId == null)
    {
        return false;
    }
    var rank = db.SortedSetRank("viewed:", itemId);
    return rank != null && rank < 1000;
}

string GetItemId(string request)
{
    throw new NotImplementedException();
}

void ScheduleRowCache(IDatabase db, int rowId, long delayInSeconds)
{
    db.SortedSetAdd("delay:", rowId, delayInSeconds);
    db.SortedSetAdd("schedule:", rowId, NowInSeconds());

}

void CacheRows(IDatabase db)
{
    var quit = false;
    while (!quit)
    {
        var next = db.SortedSetRangeByRankWithScores("schedule:", 0, 0);
        var now = NowInSeconds();

        if (next == null || next[0].Score > now)
        {
            Thread.Sleep(5);
            continue;
        }

        var rowId = next[0].Element;

        var delay = db.SortedSetScore("delay:", rowId);
        if (delay <= 0)
        {
            db.SortedSetRemove("delay:", rowId);
            db.SortedSetRemove("schedule:", rowId);
            db.KeyDelete($"inv:{rowId}");
            continue;
        }

        string row = GetJsonInventoryFromDatabase(rowId);
        db.SortedSetAdd("schedule:", rowId, now + delay.Value);
        db.StringSet($"inv:{rowId}", row);
    }
}

string GetJsonInventoryFromDatabase(RedisValue rowId)
{
    throw new NotImplementedException();
}

void RescaleViewed(IDatabase db)
{
    var quit = false;
    while (!quit)
    {
        db.SortedSetRemoveRangeByRank("viewed:", 20000, -1);
        db.SortedSetCombineAndStore(SetOperation.Intersect, "viewed:", ["viewed:"], [0.5]);
        Thread.Sleep(300);
    }
}
double NowInSeconds()
{
    return TimeSpan.FromTicks(DateTime.UtcNow.Ticks).TotalSeconds;
}