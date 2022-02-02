using System.Threading.Tasks;

namespace KafkaRetry.Job.Services.Interfaces
{
    public interface IKafkaRetryJobService
    {
        Task MoveMessages();
    }
}