using System;
using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace Ipsos.DE.Racer21.Data
{

    /// <summary>
    /// Klasse, die den Zugriff auf die Datenbank kapselt
    /// </summary>
    public class DatenzugriffSQL
	{

        public string strConn;
        private readonly IConfiguration _configuration;
		private SqlConnection objConn;	
		private SqlDataReader r;
		private string qString;
        private string projekt;

        /// <summary>
        /// Konstruktor für Queries
        /// </summary>
        /// <param name="projekt"></param>
        public DatenzugriffSQL(string projekt, IConfiguration configuration)
        {
            this._configuration=configuration;
            this.projekt = projekt;
            strConn = _configuration.GetConnectionString("DefaultConnection");
        }

        /// <summary>
        /// Konstruktor für Queries, die Abfrage wird ausgeführt, damit wird der Reader gefüllt
        /// </summary>
        /// <param name="projekt">Name des Projektes</param>
        /// <param name="q">query string</param>
        public DatenzugriffSQL(string projekt, string q, IConfiguration configuration)
        {
            this._configuration = configuration;
            this.projekt = projekt;
            strConn = _configuration.GetConnectionString("DefaultConnection");
            qString = q;
            openAbfrage(q);
        }



        /// <summary>
        /// Konstruktor für parametrisierte Queries, die Abfrage wird ausgeführt, damit wird der Reader gefüllt
        /// </summary>
        /// <param name="projekt">Name des Projektes</param>
        /// <param name="q">query string</param>
        public DatenzugriffSQL(string projekt, string q, string[] paraNamen, string[] paraTypen, string[] paraWerte, IConfiguration configuration)
        {
            this._configuration = configuration;
            this.projekt = projekt;
            strConn = _configuration.GetConnectionString("DefaultConnection");
            qString = q;
            openAbfrage(q, paraNamen, paraTypen, paraWerte);
        }




		/// <summary>
		/// öffnet eine Connection zur Datenbank
		/// </summary>
        public void openConnection(bool mitFehler, string q)
        {
            try
            {
                objConn = new SqlConnection(strConn);
                objConn.Open();
            }
            catch(Exception ex)
            {
                //Fehlerbehandlung
                if (mitFehler)
                {
                    schreibeFehler(projekt, "Fehler beim Aufbau der Connection", ex);
                }
            }
        }

        public void schreibeFehler(string s, string fehlerBeimAufbauDerConnection, Exception exception)
        {
            //TODO: log4net
        }

        /// <summary>
		/// schließt die Connection zur Datenbank
		/// </summary>
		public void closeConnection() 
		{
            if (objConn != null)
            {
                objConn.Close();
                objConn.Dispose();
                objConn = null;
            }
        }

        /// <summary>
        /// setzt eine Insert query, fragt dann die last_insert_id ab. Die Connection bleibt dabei offen...
        /// so findet man die zuletzt in die DB eingetragene ID
        /// </summary>
        /// <param name="q"></param>
        /// <returns></returns>
        public int setzeGetID(string q) 
		{
			qString = q;
			int erg;
			if ((objConn==null) || (objConn.State!=ConnectionState.Open))
			{
				openConnection(true,q);
			}
			SqlCommand objCmd = new SqlCommand(q,objConn);			
			int rowsAffected = 0;			
			try 
			{
				rowsAffected = objCmd.ExecuteNonQuery();
			}
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler beim Insert: " + q, ex);
				return 0;
			}
			SqlCommand objCmdgetID = new SqlCommand("select @@IDENTITY as id",objConn);			
			try 
			{
				r = objCmdgetID.ExecuteReader();
				r.Read();
				erg = (int)r.GetDecimal(0);
				r.Close();
			}
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler bei last_insert_id " + q, ex);
				return 0;
			}
			finally 
			{
				closeConnection();
			}
			return erg;
		}

		/// <summary>
		/// setzt eine Query, die die Datenbank ändert (z.B. insert, update, delete, ...)
		/// </summary>
		/// <param name="q"></param>
		public string setze(bool mitFehler, string q)
		{
            string erg = "";
			qString = q;
            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(mitFehler,q);
            }
            SqlCommand objCmd = new SqlCommand(q, objConn);
            objCmd.CommandTimeout = 0; 
			int rowsAffected = 0;			
			try 
			{
				rowsAffected = objCmd.ExecuteNonQuery();
			}
			catch (Exception ex)
			{
                erg = "Fehler bei " + q + ": " + ex.Message;
                if (mitFehler)
                {
                    schreibeFehler(projekt, erg, ex);
                }
            }
			finally 
			{
                closeConnection();
			}
            return erg;
		}
        /// <summary>
        /// setzt eine Query, die die Datenbank ändert (z.B. insert, update, delete, ...)
        /// </summary>
        /// <param name="q"></param>
        public string löscheBDB(bool mitFehler, string q)
        {
            string erg = "";
            qString = q;
            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(mitFehler, q);
            }
            SqlCommand objCmd = new SqlCommand(q, objConn);
            objCmd.CommandTimeout = 0;
            int rowsAffected = 0;
            try
            {
                rowsAffected = objCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {               
                //hier das constrain löschen und nochmal dropen
                string constraindb = "";
                string db = "";
                string[] split;
                split=ex.ToString().Split(' ');
                foreach(string s in split)
                {
                    if(s.StartsWith("'"))
                    {
                        constraindb = s.Replace("'", "").Trim();
                        break;
                    }
                }
                split = q.Split(' ');
                foreach(string s in split)
                {
                    if(s.ToUpper()!="ALTER" && s.ToUpper()!="TABLE")
                    {
                        db = s;
                        break;
                    }
                }
                try
                {
                    //constrain lösen
                    string qcon= "ALTER TABLE "+db+" DROP CONSTRAINT " +constraindb;
                    objCmd = new SqlCommand(qcon, objConn);
                    objCmd.CommandTimeout = 0;
                    rowsAffected = 0;
                    rowsAffected = objCmd.ExecuteNonQuery();
                    //nochmal versuchen
                    objCmd = new SqlCommand(q, objConn);
                    objCmd.CommandTimeout = 0;
                    rowsAffected = 0;
                    rowsAffected = objCmd.ExecuteNonQuery();

                }
                catch
                {
                    erg = "Fehler bei " + q + ": " + ex.Message;
                }
            }
            finally
            {
                closeConnection();
            }
            return erg;
        }
        /// <summary>
        /// setzt eine Query, die die Datenbank ändert (z.B. insert, update, delete, ...)
        /// </summary>
        /// <param name="q"></param>
        public string setzeOhneTimeOut(bool mitFehler, string q)
        {
            string erg = "";
            qString = q;
            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(mitFehler, q);
            }
            SqlCommand objCmd = new SqlCommand(q, objConn);
            int rowsAffected = 0;
            objCmd.CommandTimeout = 0;
            try
            {
                rowsAffected = objCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                erg = "Fehler bei " + q + ": " + ex.Message;
                if (mitFehler)
                {
                    schreibeFehler(projekt, erg, ex);
                }
            }
            finally
            {
                closeConnection();
            }
            return erg;
        }




		/// <summary>
		/// setzt eine parametrisiertes Kommando
		/// </summary>
		/// <param name="q"></param>
		/// <param name="paraNamen"></param>
		/// <param name="paraTypen"></param>
		/// <param name="paraWerte"></param>
		public int setzeParameter(bool mitlastID, string q, string[] paraNamen, string[] paraTypen, string[] paraWerte) 
		{
            q = q.Replace("?", "@");
            for (int n = 0; n < paraNamen.Length; n++)
            {
                paraNamen[n] = paraNamen[n].Replace("?", "@");
            }
            
            int erg = 0;
            if (mitlastID)
            {
                q = q + " SELECT SCOPE_IDENTITY()";
            }
            qString = q;

            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(true,q);
            }
            SqlCommand objCmd = new SqlCommand(q, objConn);			
			for (int n=0; n<paraNamen.Length; n++) 
			{
				string name = paraNamen[n].ToString();
				string wert = paraWerte[n].ToString();
				string typ = paraTypen[n].ToString().Trim();

				SqlParameter p = new SqlParameter(name,wert);
				if (typ=="string") 
				{
					p.SqlDbType = System.Data.SqlDbType.NVarChar;
                    p.Size = wert.Length;
                    if (p.Size == 0)
                    {
                        p.Size = 1;
                    }
				}
				if (typ=="int") 
				{
                    p.SqlDbType = System.Data.SqlDbType.Int;
				}
                if (typ == "float")
                {
                    p.Value = double.Parse(wert);
                    p.SqlDbType = System.Data.SqlDbType.Float;
                }
                if (typ == "decimal")
                {
                    p.Value = double.Parse(wert);
                    p.SqlDbType = System.Data.SqlDbType.Decimal;
                }
                if ((typ == "text")||(typ=="ntext")) 
				{
                    p.SqlDbType = System.Data.SqlDbType.NText;
                    p.Size = wert.Length;
                    if (p.Size == 0)
                    {
                        p.Size = 1;
                    }
                }
				objCmd.Parameters.Add(p);
			}
			
			objCmd.Prepare();
			int rowsAffected = 0;			
			try 
			{
                if (!mitlastID)
                {
                    rowsAffected = objCmd.ExecuteNonQuery();
                }
                else
                {
                    erg = Convert.ToInt32(objCmd.ExecuteScalar());
                }
			}
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler bei der Abfrage " + q, ex);
				closeConnection();
				return 0;
			}
            /*
			if (mitlastID) 
			{
				SqlCommand objCmdgetID = new SqlCommand("select last_insert_id()",objConn);			
				try 
				{
					r = objCmdgetID.ExecuteReader();
					r.Read();
					erg = (int)r.GetDecimal(0);
					r.Close();
				}
				catch (Exception ex)
				{
                    schreibeFehler(projekt, "Fehler bei last_insert_id " + q, ex);
					closeConnection();
					return 0;
				}
			}
            */
			closeConnection();
			return erg;
		}

		/// <summary>
		/// liefert für die übergebene Query einen DataAdapter zurück
		/// </summary>
		/// <param name="q"></param>
		/// <returns></returns>
		public System.Data.Common.DataAdapter getAdapter(string q, bool connectionSchliessen) 
		{
			qString = q;
            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(true,q);
            }
            SqlDataAdapter da; 
			try 
			{
				da = new SqlDataAdapter(q,objConn);
                SqlCommandBuilder cmdBuiler = new SqlCommandBuilder(da);
                string s = cmdBuiler.GetUpdateCommand().CommandText;
            }
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler beim Anfordern des Adapters " + q, ex);
				da=null;
			}
            if (connectionSchliessen)
            {
                closeConnection();
            }
			return da;
		}

		/// <summary>
		/// setzt den Reader entsprechende der abfrage
		/// </summary>
		/// <param name="q"></param>
		public void openAbfrage(string q) 
		{
			qString = q;
            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(true,q);
            }
            SqlCommand objCmd = new SqlCommand(q, objConn);
            objCmd.CommandTimeout= 0;
            try
            {
				//objCmd.Prepare();
				r = objCmd.ExecuteReader();
			}
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler beim Reader " + q, ex);
			}
		}



        /// <summary>
        /// setzt den Reader entsprechende der parametriesierten abfrage
        /// </summary>
        /// <param name="q"></param>
        private void openAbfrage(string q, string[] paraNamen, string[] paraTypen, string[] paraWerte)
        {
            try
            {
                q = q.Replace("?", "@");
                for (int n = 0; n < paraNamen.Length; n++)
                {
                    paraNamen[n] = paraNamen[n].Replace("?", "@");
                }
                qString = q;
                if ((objConn == null) || (objConn.State != ConnectionState.Open))
                {
                    openConnection(true, q);
                }
                SqlCommand objCmd = new SqlCommand(q, objConn);
                for (int n = 0; n < paraNamen.Length; n++)
                {
                    string name = paraNamen[n].ToString();
                    string wert = paraWerte[n].ToString();
                    string typ = paraTypen[n].ToString().Trim();

                    SqlParameter p = new SqlParameter(name, wert);
                    if (typ == "string")
                    {
                        p.SqlDbType = System.Data.SqlDbType.NVarChar;
                        p.Size = wert.Length;
                        if (p.Size == 0)
                        {
                            p.Size = 1;
                        }
                    }
                    if (typ == "int")
                    {
                        p.SqlDbType = System.Data.SqlDbType.Int;
                    }
                    if (typ == "float")
                    {
                        p.Value = double.Parse(wert);
                        p.SqlDbType = System.Data.SqlDbType.Float;
                    }
                    if (typ == "decimal")
                    {
                        p.Value = double.Parse(wert);
                        p.SqlDbType = System.Data.SqlDbType.Decimal;
                    }
                    if ((typ == "text") || (typ == "ntext"))
                    {
                        p.SqlDbType = System.Data.SqlDbType.NText;
                        p.Size = wert.Length;
                        if (p.Size == 0)
                        {
                            p.Size = 1;
                        }
                    }
                    objCmd.Parameters.Add(p);
                }

                objCmd.Prepare();
                objCmd.CommandTimeout = 0;
                r = objCmd.ExecuteReader();
            }
            catch (Exception ex)
            {
                schreibeFehler(projekt, "Fehler bei der Abfrage " + q, ex);
            }    
        }


        
        
        /// <summary>
		/// schließt den Reader, dann sie Connection
		/// </summary>
		public void close() 
		{
			if (r!=null) 
			{
				r.Close();
			}
			//closeConnection();
		}


		/// <summary>
		/// liest einen Datensatz aus dem Reader
		/// </summary>
		/// <returns></returns>
		public bool read() 
		{
			bool erg = false;
			if (r!=null) 
			{
				erg = r.Read();
			}
			return erg;
		}

		/// <summary>
		/// prüft, ob der reader geschlossen ist
		/// </summary>
		/// <returns></returns>
		private bool isClosed() 
		{
			bool erg = false;
			if (r!=null) 
			{
				erg = r.IsClosed;
			}
			return erg;
		}


		/// <summary>
		/// liefert, ob eine spallte NULL ist
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		private bool isNull(int i) 
		{
			return r.IsDBNull(i);
		}

        /// <summary>
        /// holt einen decimal-ItemValue aus dem Reader
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public decimal getDecimal(int i)
        {
            decimal wert;
            try
            {
                wert = r.GetSqlDecimal(i).Value;
            }
            catch (Exception ex)
            {
                schreibeFehler(projekt, "Fehler bei getDecimal " + qString + " Nummer:" + i, ex);
                wert = 0;
            }
            return wert;
        }

        /// <summary>
        /// holt einen double-ItemValue aus dem Reader
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public double getDouble(int i)
        {
            double wert;
            try
            {
                wert = r.GetSqlDouble(i).Value;
            }
            catch 
            {
                try
                {
                    wert = r.GetSqlInt32(i).Value;
                }
                catch (Exception ex2)
                {
                    schreibeFehler(projekt, "Fehler bei getDouble " + qString + " Nummer:" + i, ex2);
                    wert = 0;
                }
            }
            return wert;
        }

        /// <summary>
		/// holt ein float aus dem Reader
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public float getFloat(int i) 
		{
			float wert;
            try
            {
                wert = (float)r.GetDouble(i);               
            }
            catch
            {
                try
                {
                    wert = r.GetFloat(i);
                }
                catch (Exception ex)
                {
                    schreibeFehler(projekt, "Fehler bei getFloat " + qString + " Nummer:" + i, ex);
                    wert = 0;
                }
            }
			return wert;
		}

		/// <summary>
		/// holte einen int-ItemValue aus dem reader
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public int getInt32(int i) 
		{
			int wert;
			try 
			{
				wert = r.GetInt32(i);
			}
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler bei getInt " + qString + " Nummer:" + i, ex);
				wert = 0;
			}
			return wert;
		}

		/// <summary>
		/// holt einen datetime-ItemValue aus dem reader
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public DateTime getDateTime(int i) 
		{
			DateTime wert;
			try 
			{
				wert=r.GetDateTime(i);
			}
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler bei getDateTeim " + qString + " Nummer:" + i, ex);
                wert = DateTime.MinValue;
			}
			return wert;
		}

		/// <summary>
		/// holt einen string aus dem reader
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		public string getString(int i) 
		{
			string wert;
			try 
			{
				wert = r.GetString(i);
			}
			catch 
			{
                try
                {
                    if (r.IsDBNull(i)) { return ""; }
                    wert = r.GetInt32(i).ToString();
                }
                catch 
                {
                    try
                    {
                        wert = r.GetFloat(i).ToString();
                    }
                        catch (Exception ex)
                        {
                            schreibeFehler(projekt, "Fehler bei getString " + qString + " Nummer:" + i, ex);
                            wert = "";
                        }
                }
			}
			return wert;
		}

		/// <summary>
		/// holt einen einzelnen String aus der DB
		/// </summary>
		/// <param name="q">SQL Query</param>
		/// <returns>ergebnis der abfrage</returns>
		public Skalar getScalarString(string q) 
		{
			object o;
			Skalar skalar = new Skalar();
            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(true,q);
            }
            SqlCommand objCmd = new SqlCommand(q, objConn);			
			try 
			{
				o = objCmd.ExecuteScalar();
				if (o==null||o.Equals(null)) 
				{
					skalar.gueltig=false;	
				}
				else 
				{
					skalar.gueltig=true;
					skalar.stringWert=(string)o;
				}
			}
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler bei getSkalarString " + q, ex);
                skalar.gueltig = false;
			}
			finally 
			{
				closeConnection();
			}
			return skalar;
		}

		/// <summary>
		/// holt einen einzelnen integer aus der DB
		/// </summary>
		/// <param name="q">SQL Query</param>
		/// <returns>ergebnis der abfrage</returns>
		public Skalar getScalarInt32(string q) 
		{
			object o;
			Skalar skalar = new Skalar();
            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(true,q);
            }
            SqlCommand objCmd = new SqlCommand(q, objConn);			
			try 
			{
				o = objCmd.ExecuteScalar();
				if (o==null) 
				{
					skalar.gueltig=false;	
				}
				else 
				{
					skalar.gueltig=true;
					skalar.intWert=(int)o;
				}
			}
			catch (Exception ex)
			{
                schreibeFehler(projekt, "Fehler bei getSkalarInt32 " + q, ex);
				skalar.gueltig=false;
			}
			finally 
			{
				closeConnection();
			}
			return skalar;
		}

        /// <summary>
        /// holt einen einzelnen integer aus der DB
        /// </summary>
        /// <param name="q">SQL Query</param>
        /// <returns>ergebnis der abfrage</returns>
        public Skalar getScalarDouble(string q)
        {
            object o;
            Skalar skalar = new Skalar();
            if ((objConn == null) || (objConn.State != ConnectionState.Open))
            {
                openConnection(true,q);
            }
            SqlCommand objCmd = new SqlCommand(q, objConn);
            try
            {
                o = objCmd.ExecuteScalar();
                if (o == null)
                {
                    skalar.gueltig = false;
                }
                else
                {
                    skalar.gueltig = true;
                    skalar.doubleWert = (double)o;
                }
            }
            catch (Exception ex)
            {
                schreibeFehler(projekt, "Fehler bei getSkalarInt32 " + q, ex);
                skalar.gueltig = false;
            }
            finally
            {
                closeConnection();
            }
            return skalar;
        }			

	}
}
