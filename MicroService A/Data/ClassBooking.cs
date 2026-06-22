using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace MicroService_A.Data
{
    public class ClassBooking
    {
        [Key]
        public Guid Id { get; set; }

        [Required]
        public Guid ClassId { get; set; }
        [ForeignKey(nameof(ClassId))]
        public FitnessClass? FitnessClass { get; set; }

        [Required]
        public Guid MemberId { get; set; }

        [Required]
        public DateTime BookingTimestamp { get; set; }

        [Required, StringLength(50)]
        public string Status { get; set; } = "Confirmed";

        // Heavy payload field filled with mock JSON/Text configurations
        [Required]
        public string AdditionalMetadata { get; set; } = string.Empty;
    }
}
