using Microsoft.AspNetCore.Identity;

namespace Ipsos.DE.Racer21.Data
{
    public class RacerUser : IdentityUser
    {
        public int unternehmen { get; set; } = 0;
        public bool isadmin=false;
        public RacerUser() { }

        public RacerUser(string username) : base(username)
        {
        }

    }

}
