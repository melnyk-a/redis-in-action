using StackExchange.Redis;

// Hello Redis
// 1000 articles submitted each day, 50 of them are intresting
// each article can receive at least 200 up votes
// each user can vote only once for article
// after week users can't vote on article and it's scope is fixed

// TODO:
// post article
// vote article
// group articles

// Notes:
// down votes is out of scope

// Implementation:
// hash for article info:
//      - title
//      - link
//      - poster
//      - time
//      - votes
// ordered set to get most voted articles:
//      - articleId vs score
// ordered set to get most recent  articles:
//      - articleId vs time
// set for users who hava voted for articles:
//      - articleId vs userId

const int ONE_WEEK_IN_SECONDS = 7 * 86400;
const int VOTE_SCORE = 432; // 86400 / 200 = 432

ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

var articleId = PostArticle(db, "user1", "title 1", "http://goo.gl.kZUSu");
var articleId2 = PostArticle(db, "user3", "title 2", "http://goo.gl.kZUSa");
Vote(db, "user2", articleId.ToString());

var mostVoted = GetArticles(db, 1, 2, "score:");
AddAndRemoveGroups(db, articleId.ToString(), new List<string>() { "Programming" }, Enumerable.Empty<string>());
var group = GetGroupArticles(db, "Programming", 1, 5);

long PostArticle(IDatabase db, string userId, string title, string link)
{
    var articleId = db.StringIncrement("article:");

    var voted = $"voted:{articleId}";
    db.SetAdd(voted, userId);
    db.KeyExpire(voted, TimeSpan.FromSeconds(ONE_WEEK_IN_SECONDS));

    var now = NowInSeconds();
    var article = $"article:{articleId}";
    var hash = new HashEntry[] {
        new("title", title),
        new("link", link),
        new("poster", userId),
        new("time", now),
        new("votes", 1)
    };
    db.HashSet(article, hash);

    db.SortedSetAdd("score:", article, now + VOTE_SCORE);
    db.SortedSetAdd("time:", article, now);

    return articleId;
}

void Vote(IDatabase db, string userId, string articleId)
{
    var weekOffsetFromNow = NowInSeconds() - ONE_WEEK_IN_SECONDS;
    if (db.SortedSetScore("time:", articleId) < weekOffsetFromNow)
    {
        return;
    }

    var voted = $"voted:{articleId}";
    if (db.SetAdd(voted, userId))
    {
        var article = $"article:{articleId}";
        db.SortedSetIncrement("score:", article, VOTE_SCORE);
        db.HashIncrement(article, "votes", 1);
    }
}

List<HashEntry[]> GetArticles(IDatabase db, int page, int pageSize, string key)
{
    var start = (page - 1) * pageSize;
    var end = start + pageSize - 1;

    var ids = db.SortedSetRangeByRank(key, start, end, Order.Descending);
    var result = new List<HashEntry[]>();
    foreach (var id in ids)
    {
        var article = db.HashGetAll($"article:{id}");
        result.Add(article);
    }

    return result;
}

void AddAndRemoveGroups(IDatabase db, string articleId, IEnumerable<string> groupsToAdd, IEnumerable<string> groupsToRemove)
{
    var article = $"article:{articleId}";

    foreach (var group in groupsToAdd)
    {
        db.SetAdd($"group:{group}", article);
    }

    foreach (var group in groupsToRemove)
    {
        db.SetRemove($"group:{group}", article);
    }
}

List<HashEntry[]> GetGroupArticles(IDatabase db, string group, int page, int pageSize)
{
    var resultKey = $"score:{group}";
    if (!db.KeyExists(resultKey))
    {
        db.SortedSetCombineAndStore(SetOperation.Intersect, resultKey, $"group:{group}", "score:", Aggregate.Max);
        db.KeyExpire(resultKey, TimeSpan.FromSeconds(60));
    }
    return GetArticles(db, page, pageSize, resultKey);
}

double NowInSeconds()
{
    return TimeSpan.FromTicks(DateTime.UtcNow.Ticks).TotalSeconds;
}