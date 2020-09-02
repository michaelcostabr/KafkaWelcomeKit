using System;

namespace KafkaWelcomeKit.Infra
{
    public class ConsoleMessageWriter : IMessageWriter
    {
        public void Write(string message, MessageType type)
        {
            Console.WriteLine(message);
        }
    }
}
