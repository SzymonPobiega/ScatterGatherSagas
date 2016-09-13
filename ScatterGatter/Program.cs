using System;
using System.Diagnostics;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Persistence;

namespace ScatterGatter
{
    class Program
    {
        public static int NumberOfWorkItems = 10000;
        public static Stopwatch Stopwatch;

        static void Main(string[] args)
        {
            var cfg = new EndpointConfiguration("ScatterGather");
            cfg.UsePersistence<InMemoryPersistence>();
            cfg.SendFailedMessagesTo("error");
            cfg.PurgeOnStartup(true);
            cfg.UsePersistence<NHibernatePersistence>()
                .ConnectionString(@"Data Source=.\SQLEXPRESS;Initial Catalog=nservicebus;Integrated Security=True;");
            Start(cfg).GetAwaiter().GetResult();
        }

        static async Task Start(EndpointConfiguration cfg)
        {
            var endpoint = await Endpoint.Start(cfg);

            Stopwatch = Stopwatch.StartNew();

            await endpoint.SendLocal(new StartSagaMessage
            {
                SagaId = Guid.NewGuid(),
                NumberOfWorkItems = NumberOfWorkItems
            });

            Console.WriteLine("Press <enter> to exit.");
            Console.ReadLine();

            await endpoint.Stop();
        }
    }

    class FinishHandler : IHandleMessages<SagaFinished>
    {
        public Task Handle(SagaFinished message, IMessageHandlerContext context)
        {
            var time = Program.Stopwatch.ElapsedMilliseconds;

            Console.WriteLine($"Done!. In {((double)Program.NumberOfWorkItems*1000)/time}");
            return Task.FromResult(0);
        }
    }

    class MySaga : Saga<MySaga.MySagaState>, 
        IAmStartedByMessages<StartSagaMessage>,
        IHandleMessages<ScheduledWorkDone>
    {
        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<MySagaState> mapper)
        {
            mapper.ConfigureMapping<StartSagaMessage>(m => m.SagaId).ToSaga(s => s.SagaId);
            mapper.ConfigureMapping<ScheduledWorkDone>(m => m.SagaId).ToSaga(s => s.SagaId);
        }

        public async Task Handle(StartSagaMessage message, IMessageHandlerContext context)
        {
            Data.SchedulerId = message.MasterId;
            Data.ItemsToBeDone = message.NumberOfWorkItems;

            const int MinPartitionSize = 10;

            if (message.NumberOfWorkItems > MinPartitionSize)
            {
                var numberOfScheduledMessages = 0;
                var sizeOfPartition = Math.Max(Data.ItemsToBeDone/MinPartitionSize, MinPartitionSize);

                while (numberOfScheduledMessages < Data.ItemsToBeDone)
                {
                    var partitionSize = Math.Min(sizeOfPartition, Data.ItemsToBeDone - numberOfScheduledMessages);

                    await context.SendLocal(new StartSagaMessage
                    {
                        SagaId = Guid.NewGuid(),
                        MasterId = message.SagaId,
                        NumberOfWorkItems = partitionSize
                    });

                    numberOfScheduledMessages += partitionSize;
                }
            }
            else
            {
                for (var i = 0; i < message.NumberOfWorkItems; i++)
                {
                    await context.SendLocal(new ScheduleWork());
                }
            }
        }

        public async Task Handle(ScheduledWorkDone message, IMessageHandlerContext context)
        {
            Data.ItemsDone += message.ItemsDone;

            if (Data.ItemsDone == Data.ItemsToBeDone)
            {
                if (Data.SchedulerId == null)
                {
                    await ReplyToOriginator(context, new SagaFinished());
                }
                else
                {
                    await context.SendLocal(new ScheduledWorkDone
                    {
                        ItemsDone = Data.ItemsDone,
                        SagaId = Data.SchedulerId.Value
                    });
                }
                //MarkAsComplete();
            }
        }

        public class MySagaState : ContainSagaData
        {
            public virtual Guid? SchedulerId { get; set; }
            public virtual Guid SagaId { get; set; }
            public virtual int ItemsToBeDone { get; set; }
            public virtual int ItemsDone { get; set; }
        }
    }

    class Woker : IHandleMessages<ScheduleWork>
    {
        public Task Handle(ScheduleWork message, IMessageHandlerContext context)
        {
            return context.Reply(new ScheduledWorkDone
            {
                ItemsDone = 1
            });
        }
    }

    class ScheduledWorkDone : IMessage
    {
        public int ItemsDone { get; set; }
        public Guid SagaId { get; set; }
    }

    class ScheduleWork : ICommand
    {
    }

    class SagaFinished : IMessage
    {
    }

    class StartSagaMessage : ICommand
    {
        public Guid? MasterId { get; set; }
        public Guid SagaId { get; set; }
        public int NumberOfWorkItems { get; set; }
    }
}
