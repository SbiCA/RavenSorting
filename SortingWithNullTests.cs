using FluentAssertions;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.TestDriver;

namespace RavenSorting;

public class SortingWithNullTests : RavenTestDriver
{
    private IDocumentStore _documentStore;
    
    [SetUp]
    public void Setup()
    {
        _documentStore = GetDocumentStore();
        using var session = _documentStore.OpenSession();
        session.Store(new Post("posts/1", new DateTime(2021, 1, 1)));
        session.Store(new Post("posts/2", new DateTime(2021, 1, 2)));
        session.Store(new Post("posts/3", new DateTime(2021, 1, 3)));
        session.Store(new Post("posts/4", null));
        session.Store(new Post("posts/5", new DateTime(2021, 1, 5)));
        session.SaveChanges();
        new Post_ByTimeTaken().Execute(_documentStore);
        WaitForIndexing(_documentStore);
    }

    [Test]
    public void QueryAsc_ShowsPost4WithoutTimeTakenLast()
    {
        using var session = _documentStore.OpenSession();
        var posts = session.Query<Post, Post_ByTimeTaken>()
            .OrderBy(x => x.TimeTaken)
            .ToList();

        posts.Last().Id.Should().Be("posts/4");
    }
    
    [Test]
    public void QueryDesc_ShowsPost4WithoutTimeTakenLast()
    {
        using var session = _documentStore.OpenSession();
        var posts = session.Query<Post, Post_ByTimeTaken>()
            .OrderByDescending(x => x.TimeTaken)
            .ToList();

        posts.Last().Id.Should().Be("posts/4");
    }
}


public class Post_ByTimeTaken : AbstractIndexCreationTask<Post>
{
    public Post_ByTimeTaken()
    {
        Map = posts => from post in posts
            select new
            {
                TimeTaken = post.TimeTaken ?? DateTime.MaxValue
            };
    }
}
public record Post(string Id, DateTime? TimeTaken);