using UuidExtensions;

namespace Pgsqlmq.Messages;

public sealed record Message(
    Guid Id,
    string Payload)
{
    public static Message New()
    {
        var randomStringBuffer = new byte[128];

        Random.Shared.NextBytes(randomStringBuffer);

        var randomString = Convert.ToBase64String(randomStringBuffer);

        return new Message(Uuid7.Guid(), randomString);
    }
}
