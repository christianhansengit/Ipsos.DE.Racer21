using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace Ipsos.DE.Racer21.Data
{
    public class ApplicationDbContext : IdentityDbContext<RacerUser>
    {
        public DbSet<UserLogin> UserLogins { get; set; }

        public DbSet<UserFilter> UserFilters { get; set; }

        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }
    }

    public class UserLogin
    {
        public int Id { get; set; }
        public string username { get; set; }
        public DateTime LoginTime { get; set; }
        public string logintype { get; set; }        
    }

    public class UserFilter
    {
        public int Id { get; set; }
        public string userid { get; set; }
        public string filter { get; set; }
        public int aktualisierung { get; set; }
        public int year { get; set; }

    }


}