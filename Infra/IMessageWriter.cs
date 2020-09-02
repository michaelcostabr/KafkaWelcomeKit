namespace KafkaWelcomeKit.Infra
{    public interface IMessageWriter
    {
        void Write(string message, MessageType type);
    }
}
