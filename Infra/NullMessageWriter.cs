namespace KafkaWelcomeKit.Infra
{
    public class NullMessageWriter : IMessageWriter
    {
        public void Write(string message, MessageType type)
        {
            
        }
    }
}
