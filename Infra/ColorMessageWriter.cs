using System;

namespace KafkaWelcomeKit.Infra
{
    public class ColorMessageWriter : IMessageWriter
    {
        public void Write(string message, MessageType type)
        {
            if (type == MessageType.Input)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"Produtor: {message}");
            } 
            else
            {
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.WriteLine($"Consumidor: {message}");
            }

            Console.ForegroundColor = ConsoleColor.Gray;
        }
    }
}
