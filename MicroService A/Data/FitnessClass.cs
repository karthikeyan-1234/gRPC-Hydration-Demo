using System.ComponentModel.DataAnnotations;

namespace MicroService_A.Data
{
    public class FitnessClass
    {
        [Key]
        public Guid Id { get; set; }
        [Required, StringLength(100)]
        public string Name { get; set; } = string.Empty;
        [StringLength(500)]
        public string Description { get; set; } = string.Empty;
    }
}
