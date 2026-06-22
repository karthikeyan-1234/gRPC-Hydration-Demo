using Microsoft.EntityFrameworkCore;

namespace MicroService_B.Data
{
    public class ServiceBDbContext : DbContext
    {
        public ServiceBDbContext(DbContextOptions<ServiceBDbContext> options) : base(options) { }

        public DbSet<SyncedBooking> SyncedBookings => Set<SyncedBooking>();
    }
}
