using System.ComponentModel.DataAnnotations;

namespace MicroService_B.Data
{
    public class SyncedBooking
    {
        [Key]
        public Guid Id { get; set; }
        public DateTime SyncTimestamp { get; set; }
        public string RawPayload { get; set; } = string.Empty;
    }
}
