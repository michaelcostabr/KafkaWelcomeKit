using System;
using System.Collections;

namespace KafkaWelcomeKit.Infra
{
    public class DualConsoleMessageWriter : IMessageWriter
    {
        public static int MessageCapacity { get; set; }
        private static Queue messageBuffer = new Queue();

        public DualConsoleMessageWriter(int ConsoleHeight)
        {
            MessageCapacity = ConsoleHeight-8;
            messageBuffer = new Queue();
        }
        public void Write(string message, MessageType messageType)
        {
            if (messageType == MessageType.Output)
            {
                if (messageBuffer.Count > MessageCapacity)
                {
                    messageBuffer.Dequeue();
                }

                messageBuffer.Enqueue(message);                
            }

            Console.Clear();
            Console.WriteLine("===================== log ===================");
            var log = messageBuffer.ToArray();

            for (int i = 0; i < log.Length; i++)
            {
                Console.WriteLine(log[i].ToString());
            }

            if (messageType == MessageType.Input)
            {
                Console.CursorTop = MessageCapacity+5;
                Console.WriteLine("===================== entrada ===================");
                Console.WriteLine(message);
            }
        }
    }
}