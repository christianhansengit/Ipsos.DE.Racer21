namespace Ipsos.DE.Racer21.Data
{
    /// <summary>
    /// ein Skalar, d.h. ein einzelner Wert, den eine Datenbankabfrage zurückgibt
    /// </summary>
    public class Skalar
    {
        public bool gueltig = false; //true, wenn die Abfrage einen Wert ergibt
        public int intWert = 0; //ItemValue, wenn ein Integerwert abgefragt wurde
        public string stringWert = ""; //ItemValue, wenn ein String-Wert abgefragt wurde
        public double doubleWert = 0;
    }
}
