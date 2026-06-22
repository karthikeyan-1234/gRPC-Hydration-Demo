using Microsoft.EntityFrameworkCore;

namespace MicroService_A.Data
{
    public class ServiceADbContext : DbContext
    {
        public ServiceADbContext(DbContextOptions<ServiceADbContext> options) : base(options) { }

        public DbSet<FitnessClass> FitnessClasses => Set<FitnessClass>();
        public DbSet<ClassBooking> ClassBookings => Set<ClassBooking>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Explicitly index the streaming tracking keys for maximum extraction performance
            modelBuilder.Entity<ClassBooking>()
                .HasIndex(b => b.BookingTimestamp);

            modelBuilder.Entity<ClassBooking>()
                .HasIndex(b => b.ClassId);
        }
    }
}
