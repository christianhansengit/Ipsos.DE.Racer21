using System.Data;
using Microsoft.Data.SqlClient;
using static Ipsos.DE.Racer21.Data.WSRacer;
//using static Racer2.Pages.GroupSelect;


namespace Ipsos.DE.Racer21.Data
{
    public class WSRacer
    {
        public int itemtotal = 0;
        public int itemcount = 0;
        private readonly IConfiguration _configuration;

        public WSRacer(IConfiguration configuration)
        {
            this._configuration= configuration;
        }

        /// <summary>
        /// liefert die Liste aller Items jewiels gruppiert nach den Indizes / Subindizes
        /// </summary>
        /// <returns></returns>
        public List<BMItemIndex> GetItems(string sprache)
        {
            List<BMItemIndex> erg = new List<BMItemIndex>();
            List<BMItemIndex> indexList = getAlleIndizes(sprache);
            foreach (BMItemIndex index in indexList)
            {
                index.typ = "index";
                erg.Add(index);
                List<BMItemIndex> itemlist = getAlleItemsInIndex(index.id,sprache);
                foreach (BMItemIndex item in itemlist)
                {
                    item.typ = "item";
                    erg.Add(item);
                }
                List<BMItemIndex> subindexlist = getAlleSubIndizesInINdex(index.id,sprache);
                foreach (BMItemIndex subindex in subindexlist)
                {
                    subindex.typ = "subindex";
                    erg.Add(subindex);
                    List<BMItemIndex> iteminsublist = getAlleItemsInSubIndex(subindex.id,sprache);
                    foreach (BMItemIndex item in iteminsublist)
                    {
                        item.typ = "item";
                        erg.Add(item);
                    }
                }
            }
            //Items ohen Index gibt es nihct mehr
            //List<BMItemIndex> itemohneIndexlist = getAlleItemsOhneIndex(sprache);
            //foreach (BMItemIndex item in itemohneIndexlist)
            //{
            //    item.typ = "itemohneindex";
            //    erg.Add(item);
            //}

            return erg;
        }

        /// <summary>
        /// liefert das Unternehmen, basierend auf seiner ID
        /// </summary>
        /// <param name="unternehmenID"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
        public Unternehmen getUnternehmen(int unternehmenID)
        {
            Unternehmen erg = null;
            string q = "SELECT [ID],[Name],mwnachkomma,[cutoffCompanies] FROM [SISRacerDBNeu].[dbo].[Unternehmen] WHERE [ID]=" + unternehmenID.ToString();
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            if (r.read())
            {
                erg = new Unternehmen();
                erg.id = r.getInt32(0);
                erg.name = r.getString(1);
                erg.nachkommastellen = r.getInt32(2);
                erg.companyCutOff = r.getInt32(3);

            }
            r.close(); r.closeConnection();
            if (erg == null)
            {
                erg = new Unternehmen();
                erg.id = 0;
                erg.name = "Test AG";
                erg.nachkommastellen = 2;
                erg.companyCutOff = 3;
            }

            if (erg.name == "")
            {
                erg.name = "Test AG";
            }
            return erg;
        }


        /// <summary>
        /// liefert die Liste aller Unternhemen
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>

        public List<Unternehmen> getAlleUnternehmen()
        {
            List<Unternehmen> erg = new List<Unternehmen>();
            string q = "SELECT [ID],[Name],mwnachkomma FROM [SISRacerDBNeu].[dbo].[Unternehmen]";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                Unternehmen u = new Unternehmen();
                u.id = r.getInt32(0);
                u.name = r.getString(1);
                u.nachkommastellen = r.getInt32(2);
                erg.Add(u);
            }
            r.close(); r.closeConnection();

            return erg;
        }


        /// <summary>
        /// liefert alle Indizes
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>

        public List<BMItemIndex> getAlleIndizes(string sprache)
        {

            List<BMItemIndex> erg = new List<BMItemIndex>();

            //liefere die neuen Indizes

            string q = "SELECT [ID] ,[Code] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMIndex] where id>=10 order by id";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                BMItemIndex bmItemIndex = new BMItemIndex();
                bmItemIndex.id = r.getInt32(0);
                bmItemIndex.code = r.getString(1);
                if (sprache == "en")
                {
                    bmItemIndex.text = r.getString(3);
                }
                else
                {
                    bmItemIndex.text = r.getString(2);
                }
                erg.Add(bmItemIndex);
            }
            r.close(); r.closeConnection();
            //die alten Indizes werden nicht mehr berücksichtigt
            /*
            q = "SELECT [ID] ,[Code] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMIndex] where id<10 order by id";
            r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                BMItemIndex bmItemIndex = new BMItemIndex();
                bmItemIndex.id = r.getInt32(0);
                bmItemIndex.code = r.getString(1);
                string textD = r.getString(2);
                string textE = r.getString(3);
                if (sprache == "d")
                {
                    bmItemIndex.text = textD;
                }
                else
                {
                    bmItemIndex.text = textE;
                }
                erg.Add(bmItemIndex);
            }
            r.close(); r.closeConnection();
            */
            return erg;
        }

        /// <summary>
        /// liefert alle Subindizes aus einem Index
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>

        public List<BMItemIndex> getAlleSubIndizesInINdex(int indexID, string sprache)
        {

            List<BMItemIndex> erg = new List<BMItemIndex>();

            string q = "SELECT [ID] ,[Code] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMSubIndex] where BMIndex=" + indexID + " order by id";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                BMItemIndex bmItemIndex = new BMItemIndex();
                bmItemIndex.id = r.getInt32(0);
                bmItemIndex.code = r.getString(1);
                if (sprache == "en")
                {
                    bmItemIndex.text = r.getString(3);
                }
                else
                {
                    bmItemIndex.text = r.getString(2);
                }
                erg.Add(bmItemIndex);
            }
            r.close(); r.closeConnection();
            return erg;
        }

        /// <summary>
        /// liefert alle gespeicherten aktualisierungen
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>

        public List<Aktualisierung> getAlleAktualisierungen()
        {

            List<Aktualisierung> erg = new List<Aktualisierung>();
            string q = "SELECT [ID],[Datum] FROM Aktualisierung order by [ID] DESC";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                Aktualisierung akt = new Aktualisierung();
                akt.id = r.getInt32(0);
                akt.datum = r.getString(1);
                erg.Add(akt);
            }
            r.close();
            r.closeConnection();
            return erg;
        }

        /// <summary>
        /// liefert eine aktualisierung
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public Aktualisierung getAktualisierung(int id)
        {
            Aktualisierung akt = null;
            string q = "SELECT [ID],[Datum] FROM Aktualisierung where [ID]=" + id;
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            if (r.read())
            {
                akt = new Aktualisierung();
                akt.id = r.getInt32(0);
                akt.datum = r.getString(1);
            }
            r.close();
            r.closeConnection();
            return akt;
        }

        /// <summary>
        /// liefert alle jahre, aus denen aktualisierte Befragungen vorliegen
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<int> getAlleJahre()
        {
            List<int> erg = new List<int>();
            string q = "SELECT distinct year FROM Befragung order by year desc";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                erg.Add(r.getInt32(0));
            }
            r.close();
            r.closeConnection();
            return erg;
        }

        /// <summary>
        /// liefert alle befragungen
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<Befragung> getAlleBefragungen()
        {
            List<Befragung> erg = new List<Befragung>();
            string q = "SELECT Befragung.ID, Befragung.Datum, Befragung.Teilnehmer, Befragung.Vollbefragung, Befragung.UnternehmenID, Unternehmen.Name, Befragung.NeueNummer, Befragung.Rücklauf, Befragung.Cycle, Befragung.Cutoff, Befragung.QuotePaper, Befragung.QuoteOnline FROM Befragung INNER JOIN Unternehmen ON Befragung.UnternehmenID = Unternehmen.ID  order by Befragung.ID DESC";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                Befragung b = new Befragung();
                b.id = r.getInt32(0);
                b.datum = r.getString(1);
                b.teilnehmer = r.getInt32(2);
                b.vollbefragung = (r.getInt32(3) == 1);
                b.unternehmenID = r.getInt32(4);
                b.unternehmenName = r.getString(5);
                b.aktualisierungsnummer = r.getInt32(6);
                b.rücklauf = r.getString(7);
                b.cycle = r.getString(8);
                b.cutOff = r.getInt32(9);
                b.quotePaper = r.getString(10);
                b.quoteOnline = r.getString(11);
                erg.Add(b);
            }
            r.close();
            r.closeConnection();
            return erg;
        }

        public List<Befragung> getAlleBefragungen(List<int> ids)
        {
            //jedes Unternehmen nur einmal
            List<int> unternehemnsIds = new List<int>();

            List<Befragung> erg = new List<Befragung>();
            string q = "SELECT Befragung.ID, Befragung.Datum, Befragung.Teilnehmer, Befragung.Vollbefragung, Befragung.UnternehmenID, " +
                       "Unternehmen.Name, Befragung.NeueNummer, Befragung.Rücklauf, Befragung.Cycle, Befragung.Cutoff, Befragung.QuotePaper, Befragung.QuoteOnline " +
                       "FROM Befragung INNER JOIN Unternehmen ON Befragung.UnternehmenID = Unternehmen.ID  " +
                       "WHERE Befragung.ID in ("+string.Join(",",ids)+")" +
                       "order by Befragung.ID DESC";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                Befragung b = new Befragung();
                b.id = r.getInt32(0);
                b.datum = r.getString(1);
                b.teilnehmer = r.getInt32(2);
                b.vollbefragung = (r.getInt32(3) == 1);
                b.unternehmenID = r.getInt32(4);
                b.unternehmenName = r.getString(5);
                b.aktualisierungsnummer = r.getInt32(6);
                b.rücklauf = r.getString(7);
                b.cycle = r.getString(8);
                b.cutOff = r.getInt32(9);
                b.quotePaper = r.getString(10);
                b.quoteOnline = r.getString(11);
                if (!unternehemnsIds.Contains(b.unternehmenID))
                {
                    erg.Add(b);
                    unternehemnsIds.Add(b.unternehmenID);
                }
            }
            r.close();
            r.closeConnection();
            return erg;
        }



        /// <summary>
        /// liefert alle abweichenden Skalen zur aktuellen Befragung
        /// </summary>
        /// <param name="unternehmen"></param>
        /// <param name="aktualisierung"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>

        public List<Skala> getSkalenUnternehmen(int unternehmen, int aktualisierung)
        {
            List<Skala> erg = new List<Skala>();

            string q = "SELECT[BefragungID] FROM[SISRacerDBNeu].[dbo].[BefragungAktualisierung]  inner join Befragung on Befragung.ID = BefragungAktualisierung.BefragungID  where Aktualisierung = " + aktualisierung + " and Befragung.UnternehmenID = " + unternehmen;
            int befragungId = -1;
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            if (r.read())
            {
                befragungId= r.getInt32(0);
            }
            r.close();

            if (befragungId>0)
            {
                q = "select distinct SkalaMin,SkalaMax from Item where Item.BefragungID=" + befragungId;
                r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
                if (r.read())
                {
                    int skalamin = r.getInt32 (0);
                    int skalamax = r.getInt32 (1);
                    if (skalamin!=1 || skalamax !=5)
                    {
                        erg.Add (new Skala { min=skalamin, max=skalamax });
                    }
                }
                r.close();

            }
            r.closeConnection();
            return erg;
        }


        /// <summary>
        /// liefert alle befragungen in einer bestimmten aktualisierung
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<int> getAlleBefragungenIDsInAktualisierung(int aktualisierung)
        {

            List<int> erg = new List<int>();
            string q = "SELECT [BefragungID] FROM [SISRacerDBNeu].[dbo].[BefragungAktualisierung] WHERE [Aktualisierung]=" + aktualisierung.ToString();
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                erg.Add(r.getInt32(0));
            }
            r.close();
            r.closeConnection();
            return erg;
        }

        /// <summary>
        /// liefert alle befragungen in einem Jahr
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<int> getAlleBefragungenIDsInJahr(int jahr)
        {

            List<int> erg = new List<int>();
            string q = "SELECT ID FROM [SISRacerDBNeu].[dbo].[Befragung] WHERE [Year]=" + jahr;
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                erg.Add(r.getInt32(0));
            }
            r.close();
            r.closeConnection();
            return erg;
        }



        /// <summary>
        /// liefert zu einer befragung die einzelnen Items
        /// </summary>
        /// <param name="Befragung"></param>
        /// <param name="indexID"></param>
        /// <param name="subindexID"></param>
        /// <param name="sprache"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>

        //public List<string> getEinzelItems(int Befragung, int bmItemID, string sprache)
        //{
        //    List<string> erg = new List<string>();
        //    string where = "";
        //    int id = 0;
        //    if (bmItemID > 0) { where = " AND [BMItemID]=@id"; id = bmItemID; }

        //    string[] paraNamen = new string[2] { "@BefragungID", "@id" };
        //    string[] paraWerte = new string[2] { Befragung.ToString(), id.ToString() };
        //    string[] paraTypen = new string[2] { "int", "int" };
        //    string q = "SELECT [TextD],[TextE] FROM [SISRacerDBNeu].[dbo].[Item] where [BefragungID]=@BefragungID " + where;
        //    Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, paraNamen, paraTypen, paraWerte, _configuration);
        //    while (r.read())
        //    {
        //        if (sprache.ToLower() == "d")
        //        {
        //            erg.Add(r.getString(0));
        //        }
        //        else
        //        {
        //            erg.Add(r.getString(1));
        //        }
        //    }
        //    r.close(); r.closeConnection();
        //    return erg;
        //}


        /// <summary>
        /// liefert alle demografischen Variablen zur anzeige
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>

        public List<AnzeigeDemoMerkmal> getAlleAnzeigeDemografischen()
        {

            List<AnzeigeDemoMerkmal> erg = new List<AnzeigeDemoMerkmal>();
            string q = "select [id],[textd],[texte] from DemoMerkmal order by [ID]";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                AnzeigeDemoMerkmal dm = new AnzeigeDemoMerkmal();
                dm.id = r.getInt32(0);
                dm.textd = r.getString(1);
                dm.texte = r.getString(2);
                erg.Add(dm);
            }
            r.close(); r.closeConnection();
            return erg;
        }

        /// <summary>
        /// liefert alle demografischen Variablen zum Filtern
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<FilterDemoMerkmal> getAlleFilterDemografischen()
        {

            List<FilterDemoMerkmal> erg = new List<FilterDemoMerkmal>();
            string q = "select [id],[merkmalid],[kürzel] from FilterMerkmal order by [ID]";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                FilterDemoMerkmal dm = new FilterDemoMerkmal();
                dm.id = r.getInt32(0);
                dm.anzeigeMerkmal = r.getInt32(1);
                dm.kürzel = r.getString(2);
                erg.Add(dm);
            }
            r.close(); r.closeConnection();
            return erg;
        }

        /// <summary>
        /// leifert alle kategorien zu einer demografischen Variablen
        /// </summary>
        /// <param name="merkmal"></param>
        /// <param name="nachAnzeige">wenn true, dann sollen alle Kategorien nach einen Anzeigemerkmal gezeigt werden, z.b. Region, sonst nach einen Filter, also z.b. region1, region2, region3</param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<DemoKategorie> getAlleDemoKategorien(int merkmal, bool nachAnzeige)
        {

            List<DemoKategorie> erg = new List<DemoKategorie>();

            string q;
            //sortiert nach anzahl der nennungen
            //q = "select [id],[textd],[texte],[anzNennungen] from DemoKategorie WHERE MerkmalID=" + merkmal + " ORDER BY anzNennungen DESC, id";            
            if (nachAnzeige)
            {
                q = "select [id],[textd],[texte],[anzNennungen],anzeigeMerkmalID,filterMerkmalID,level from DemoKategorie WHERE anzeigeMerkmalID=" + merkmal + " ORDER BY nr";
            }
            else
            {
                q = "select [id],[textd],[texte],[anzNennungen],anzeigeMerkmalID,filterMerkmalID,level from DemoKategorie WHERE filterMerkmalID=" + merkmal + " ORDER BY nr";
            }
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                DemoKategorie dk = new DemoKategorie();
                dk.id = r.getInt32(0);
                dk.textd = r.getString(1);
                dk.texte = r.getString(2);
                dk.anzNennungen = r.getInt32(3);
                dk.anzeigemerkmal = r.getInt32(4);
                dk.filtermerkmal = r.getInt32(5);
                dk.level = r.getInt32(6);
                erg.Add(dk);
            }
            r.close(); r.closeConnection();
            return erg;
        }

        /// <summary>
        /// liefert den Text einer Kategorie
        /// </summary>
        /// <param name="katId"></param>
        /// <param name="filterMerkmalId"></param>
        /// <param name="sprache"></param>
        /// <returns></returns>
        public string getKatText(int katId, int filterMerkmalId, string sprache)
        {
            string text = "";
            string q = "";
            if (sprache == "en")
            {
                q = "select TextE from Demokategorie where id=" + katId + " and filterMerkmalID=" + filterMerkmalId;
            }
            else
            {
                q = "select TextD from Demokategorie where id=" + katId + " and filterMerkmalID=" + filterMerkmalId;
            }

            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
                if (r.read())
                {
                    text = r.getString(0);
                }
                r.close(); r.closeConnection();

            return text;
        }

    

        /// <summary>
        /// liefert alle Standardmapping
        /// BMItem->index
        /// BMItem->subindex
        /// subindex->index
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<MappingIndex> getAlleMappings()
        {

            List<MappingIndex> erg = new List<MappingIndex>();
            string q;

            //lese alle Indizes
            q = "select [ID],[Code],[TextD],[TextE] from BMIndex ORDER BY [ID]";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                MappingIndex indMap = new MappingIndex();
                indMap.id = r.getInt32(0);
                indMap.code = r.getString(1).ToLower();
                indMap.textD = r.getString(2);
                indMap.textE = r.getString(3);
                erg.Add(indMap);
            }
            r.close(); r.closeConnection();

            Dictionary<int, MappingSubIndex> subIndDict = new Dictionary<int, MappingSubIndex>();
            //lese alle subindizes
            q = "select [ID],[Code],[TextD],[TextE] from BMSubIndex ORDER BY [ID]";
            r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                MappingSubIndex indMap = new MappingSubIndex();
                indMap.id = r.getInt32(0);
                indMap.code = r.getString(1).ToLower();
                indMap.textD = r.getString(2);
                indMap.textE = r.getString(3);
                subIndDict.Add(indMap.id, indMap);
            }
            r.close(); r.closeConnection();

            //ordne jedem SubIndex seine BMItems zu
            foreach (MappingSubIndex subInd in subIndDict.Values)
            {
                q = "select [itemid] from [BMItemIndex] WHERE subindexid=" + subInd.id;
                r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
                while (r.read())
                {
                    subInd.items.Add(r.getInt32(0));
                }
                r.close(); r.closeConnection();
            }

            //ordne jedem Index seine BMItems und subindizes zu
            foreach (MappingIndex ind in erg)
            {
                q = "select [itemid] from [BMItemIndex] WHERE [indexid]=" + ind.id;
                r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
                while (r.read())
                {
                    ind.items.Add(r.getInt32(0));
                }
                r.close(); r.closeConnection();
                q = "select [ID] from BMSubIndex WHERE BMIndex=" + ind.id;
                r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
                while (r.read())
                {
                    ind.subindizes.Add(subIndDict[r.getInt32(0)]);
                }
                r.close(); r.closeConnection();
            }

            return erg;
        }


        public string ListeToKomma(string[] liste, char trennzeichen)
        {
            string erg = "";
            foreach (string s in liste)
            {
                if (erg != "")
                {
                    erg = erg + trennzeichen;
                }
                erg = erg + s;
            }
            return erg;
        }




        /// <summary>
        /// liefert einen Index
        /// </summary>
        /// <param name="id"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        //public BMItemIndex getIndex(int id, string sprache)
        //{

        //    BMItemIndex bmItemIndex = null;

        //    string q = "SELECT [ID] ,[Code] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMIndex] WHERE [ID]=" + id;
        //    Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
        //    if (r.read())
        //    {
        //        bmItemIndex = new BMItemIndex();
        //        bmItemIndex.id = r.getInt32(0);
        //        bmItemIndex.code = r.getString(1);
        //        string textD = r.getString(2);
        //        string textE = r.getString(3);
        //        if (sprache == "d")
        //        {
        //            bmItemIndex.text = textD;
        //        }
        //        else
        //        {
        //            bmItemIndex.text = textE;
        //        }
        //    }
        //    r.close(); r.closeConnection();
        //    return bmItemIndex;
        //}

        /// <summary>
        /// liefert einen SubIndex
        /// </summary>
        /// <param name="id"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        //public BMItemIndex getSubIndizes(int id, string sprache)
        //{

        //    BMItemIndex bmItemIndex = null;

        //    string q = "SELECT [ID] ,[Code] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMSubIndex] WHERE [ID]=" + id;
        //    Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
        //    if (r.read())
        //    {
        //        bmItemIndex = new BMItemIndex();
        //        bmItemIndex.id = r.getInt32(0);
        //        bmItemIndex.code = r.getString(1);
        //        string textD = r.getString(2);
        //        string textE = r.getString(3);
        //        if (sprache == "d")
        //        {
        //            bmItemIndex.text = textD;
        //        }
        //        else
        //        {
        //            bmItemIndex.text = textE;
        //        }
        //    }
        //    r.close(); r.closeConnection();
        //    return bmItemIndex;
        //}


        /// <summary>
        /// liefert alle BMItems
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        //public BMItemIndex getItem(int id, string sprache)
        //{

        //    BMItemIndex bmItemIndex = null;

        //    string q = "SELECT [ID] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMItem] WHERE [ID]=" + id;
        //    Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
        //    if (r.read())
        //    {
        //        bmItemIndex = new BMItemIndex();
        //        bmItemIndex.id = r.getInt32(0);
        //        bmItemIndex.code = "";
        //        string textD = r.getString(1);
        //        string textE = r.getString(2);
        //        if (sprache == "d")
        //        {
        //            bmItemIndex.text = textD;
        //        }
        //        else
        //        {
        //            bmItemIndex.text = textE;
        //        }
        //    }
        //    r.close(); r.closeConnection();
        //    return bmItemIndex;
        //}


 
        /// <summary>
        /// liefert alle Subindizes
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        //public List<BMItemIndex> getAlleSubIndizes(string sprache)
        //{

        //    List<BMItemIndex> erg = new List<BMItemIndex>();

        //    string q = "SELECT [ID] ,[Code] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMSubIndex] order by id";
        //    Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
        //    while (r.read())
        //    {
        //        BMItemIndex bmItemIndex = new BMItemIndex();
        //        bmItemIndex.id = r.getInt32(0);
        //        bmItemIndex.code = r.getString(1);
        //        string textD = r.getString(2);
        //        string textE = r.getString(3);
        //        if (sprache == "d")
        //        {
        //            bmItemIndex.text = textD;
        //        }
        //        else
        //        {
        //            bmItemIndex.text = textE;
        //        }
        //        erg.Add(bmItemIndex);
        //    }
        //    r.close(); r.closeConnection();
        //    return erg;
        //}


        /// <summary>
        /// liefert alle BMItems
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        //public List<BMItemIndex> getAlleItems(string sprache)
        //{

        //    List<BMItemIndex> erg = new List<BMItemIndex>();

        //    string q = "SELECT [ID] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMItem] order by ID";
        //    Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
        //    while (r.read())
        //    {
        //        BMItemIndex bmItemIndex = new BMItemIndex();
        //        bmItemIndex.id = r.getInt32(0);
        //        bmItemIndex.code = "";
        //        string textD = r.getString(1);
        //        string textE = r.getString(2);
        //        if (sprache == "d")
        //        {
        //            bmItemIndex.text = textD;
        //        }
        //        else
        //        {
        //            bmItemIndex.text = textE;
        //        }
        //        erg.Add(bmItemIndex);
        //    }
        //    r.close(); r.closeConnection();
        //    return erg;
        //}

        /// <summary>
        /// liefert alle BMItems aus einem Subindex
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<BMItemIndex> getAlleItemsInSubIndex(int subindexID, string sprache)
        {

            List<BMItemIndex> erg = new List<BMItemIndex>();

            string q = "SELECT items.[ID] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMItem] as items, bmitemIndex as itemindex where items.id=itemindex.itemid and itemindex.subindexid=" + subindexID + " order by items.ID";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                BMItemIndex bmItemIndex = new BMItemIndex();
                bmItemIndex.id = r.getInt32(0);
                bmItemIndex.code = "";
                if (sprache == "en")
                {
                    bmItemIndex.text = r.getString(2);
                }
                else
                {
                    bmItemIndex.text = r.getString(1);
                }
                erg.Add(bmItemIndex);
            }
            r.close(); r.closeConnection();
            return erg;
        }

        /// <summary>
        /// liefert alle BMItems aus einem Index
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<BMItemIndex> getAlleItemsInIndex(int indexID, string sprache)
        {

            List<BMItemIndex> erg = new List<BMItemIndex>();

            string q = "SELECT items.[ID] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMItem] as items, bmitemIndex as itemindex where items.id=itemindex.itemid and itemindex.indexid=" + indexID + " order by items.ID";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                BMItemIndex bmItemIndex = new BMItemIndex();
                bmItemIndex.id = r.getInt32(0);
                bmItemIndex.code = "";
                if (sprache == "en")
                {
                    bmItemIndex.text = r.getString(1);
                }
                else
                {
                    bmItemIndex.text = r.getString(1);
                }
                erg.Add(bmItemIndex);
            }
            r.close(); r.closeConnection();
            return erg;
        }

        /// <summary>
        /// liefert alle BMItems die nicht zugeordnet sind
        /// </summary>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public List<BMItemIndex> getAlleItemsOhneIndex(string sprache)
        {

            List<BMItemIndex> erg = new List<BMItemIndex>();

            string q = "SELECT items.[ID] ,[TextD] ,[TextE] FROM [SISRacerDBNeu].[dbo].[BMItem] as items, bmitemIndex as itemindex where items.id=itemindex.itemid and itemindex.indexid=0 and itemindex.subindexid=0  order by items.ID";
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            while (r.read())
            {
                BMItemIndex bmItemIndex = new BMItemIndex();
                bmItemIndex.id = r.getInt32(0);
                bmItemIndex.code = "";
                if (sprache == "en")
                {
                    bmItemIndex.text = r.getString(2);
                }
                else
                {
                    bmItemIndex.text = r.getString(1);
                }
                erg.Add(bmItemIndex);
            }
            r.close(); r.closeConnection();
            return erg;
        }



        /// <summary>
        /// liefert zu einer BefragungsID das zugehörige Unternhemen
        /// </summary>
        /// <param name="befragung"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public Unternehmen getBefragungUnternhemen(int befragung)
        {


            Unternehmen erg = new Unternehmen();
            string q = "SELECT Unternehmen.Name, Unternehmen.ID FROM Befragung INNER JOIN Unternehmen ON Befragung.UnternehmenID = Unternehmen.ID where befragung.id = " + befragung;
            Data.DatenzugriffSQL r = new Data.DatenzugriffSQL("RacerDBNeu", q, _configuration);
            if (r.read())
            {
                erg.id = r.getInt32(1);
                erg.name = r.getString(0);
            }
            r.close(); r.closeConnection();
            return erg;
        }


        /// <summary>
        /// liefert einen Kompletten benchmark satz
        /// </summary>
        /// <param name="unternehmenID"></param>
        /// <param name="befragungen"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public async Task<List<BMItemErg>> getGesamtBenchmark(int unternehmenID, int aktualisierung, int[] befragungen, List<BMItemIndex>items, string filter, int minanz, int cutoff)
        {
            Dictionary<int, Unternehmen> befragungUnternehmen = new Dictionary<int, Unternehmen>();
            foreach (int befrID in befragungen)
            {
                Unternehmen unt = getBefragungUnternhemen(befrID);
                befragungUnternehmen.Add(befrID, unt);
            }

            /*
            erg.indizes = getAlleIndizes(sprache);
            erg.subIndizes = getAlleSubIndizes(sprache);
            erg.items = getAlleItems(sprache);
            erg.indexErg = new List<BMItemErg>();
            erg.subIndexErg = new List<BMItemErg>();
            erg.itemErg = new List<BMItemErg>();
            */
            filter = toFilter(filter);
            string q;
            q = "SELECT BefragungID,BMIndex,BMSubIndex,BMItem,Mittelwert, Strg, Ja, Nein, Anz, P1, P2, P3, P4, P5 " +
                "FROM Ergebnis WHERE ";
            q = q + filter +
                " AND BefragungID in ("+string.Join(",",befragungen)+")";
            string connString = _configuration.GetConnectionString("DefaultConnection");
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(q, conn);
            conn.Open();
            DataTable dataTable = new DataTable(); 

            // create data adapter
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            // this will query your database and return the result to your datatable
            da.Fill(dataTable);
            conn.Close();
            da.Dispose();



            Data.DatenzugriffSQL dat = new Data.DatenzugriffSQL("RacerDBNeu", _configuration);
            dat.openConnection(true, "");

            List<BMItemErg> erg = new List<BMItemErg>();

            this.itemtotal = items.Count;
            this.itemcount = 0;
            foreach (BMItemIndex item in items)
            {
                itemcount++;
                BMItemErg tempErg = null;
                if (item.typ == "index")
                {
                    tempErg = getBenchmarkItemErg(dataTable, unternehmenID, befragungen, filter, item.id, 0, 0, minanz, cutoff, befragungUnternehmen);
                }
                if (item.typ == "subindex")
                {
                    tempErg = getBenchmarkItemErg(dataTable, unternehmenID, befragungen, filter, 0, item.id, 0, minanz, cutoff, befragungUnternehmen);
                }
                if (item.typ == "item" || item.typ == "itemohneindex")
                {
                    tempErg = getBenchmarkItemErg(dataTable, unternehmenID, befragungen, filter, 0, 0, item.id, minanz, cutoff, befragungUnternehmen);
                }
                erg.Add(tempErg);

                await Task.Delay(10);
            }
             dat.closeConnection();
             return erg;
        }

        /// <summary>
        /// liefert die Ergebnisse zu einer Reihe von Befragungen
        /// </summary>
        /// <param name="befragungen"></param>
        /// <param name="stat"></param>
        /// <param name="BMIndex"></param>
        /// <param name="BMSubIndex"></param>
        /// <param name="BMItem"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
        public BMItemErg getBenchmarkItemErg(DataTable dat, int unternehmenID, int[] befragungen, string filter, int BMIndex, int BMSubIndex, int BMItem, int minanz, int cutoff, Dictionary<int, Unternehmen> BefragungUnternehmen)
        {
            BMItemErg erg = new BMItemErg();
            try
            {
                erg.gesamt = new ItemErg();
                erg.eigenes = null;
                erg.alleBefragungen = new List<ItemErg>();
                erg.rangplatz = -1;
                int anz = 0;
                int anzskala = 0;
                Random rand = new Random();
                int zufall = rand.Next(1, befragungen.Length);
                foreach (int b in befragungen)
                {
                    zufall--;
                    ItemErg tempItemErg = getBenchmarkItemBefragungErg(dat,b, filter, BMIndex, BMSubIndex, BMItem, cutoff);
                    if (tempItemErg != null && tempItemErg.anzahl>=cutoff)
                    {
                        tempItemErg.unternehmen = BefragungUnternehmen[b];
                        if ((tempItemErg.unternehmen.id == unternehmenID) || (unternehmenID == 0 && zufall >= 0 &&tempItemErg.anzahl>=cutoff))
                        {
                            erg.eigenes = tempItemErg;
                            erg.eigenes.anzahl = tempItemErg.anzahl;
                        }
                        anz++;
                        erg.gesamt.mittel = erg.gesamt.mittel + tempItemErg.mittel;
                        erg.gesamt.strg = erg.gesamt.strg + tempItemErg.strg;
                        erg.gesamt.ja = erg.gesamt.ja + tempItemErg.ja;
                        erg.gesamt.nein = erg.gesamt.nein + tempItemErg.nein;
                        erg.gesamt.anzahl = erg.gesamt.anzahl + tempItemErg.anzahl;
                        if (tempItemErg.skala[0] >= 0)
                        {
                            anzskala++;
                            for (int n = 0; n < 5; n++)
                            {
                                erg.gesamt.skala[n] = erg.gesamt.skala[n] + tempItemErg.skala[n];
                            }
                        }
                        if (tempItemErg.mittel > erg.gesamt.maxmittel) { erg.gesamt.maxmittel = tempItemErg.mittel; }
                        if (tempItemErg.mittel < erg.gesamt.minmittel) { erg.gesamt.minmittel = tempItemErg.mittel; }
                        if (tempItemErg.ja > erg.gesamt.maxja) { erg.gesamt.maxja = tempItemErg.ja; }
                        if (tempItemErg.nein > erg.gesamt.maxnein) { erg.gesamt.maxnein = tempItemErg.nein; }
                        if (tempItemErg.ja < erg.gesamt.minja) { erg.gesamt.minja = tempItemErg.ja; }
                        if (tempItemErg.nein < erg.gesamt.minnein) { erg.gesamt.minnein = tempItemErg.nein; }
                        erg.alleBefragungen.Add(tempItemErg);
                    }
                }

                if (anz < minanz)
                {
                    erg.gültig=false;
                    erg.gesamt.mittel = 0;
                    erg.gesamt.strg = 0;
                    erg.gesamt.ja = 0;
                    erg.gesamt.nein = 0;
                    for (int n = 0; n < 5; n++)
                    {
                            erg.gesamt.skala[n] = 0;
                    }
                }
                else
                {
                    erg.gültig = true;
                    erg.gesamt.mittel = erg.gesamt.mittel / anz;
                    erg.gesamt.strg = erg.gesamt.strg / anz;
                    erg.gesamt.ja = erg.gesamt.ja / anz;
                    erg.gesamt.nein = erg.gesamt.nein / anz;
                    for (int n = 0; n < 5; n++)
                    {
                        if (anzskala > 0)
                        {
                            erg.gesamt.skala[n] = erg.gesamt.skala[n] / anzskala;
                        }
                        else
                        {
                            erg.gesamt.skala[n] = 0;
                        }
                    }


                    erg.gesamt.anzBefragungen = anz;
                    //sortiere alle Befragungen
                    erg.alleBefragungen.Sort(compareErg);

                    //bestimme den Rangplatz des eigenen Unternehmens
                    if (erg.eigenes != null)
                    {
                        int nummer = 0;
                        foreach (ItemErg itenErg in erg.alleBefragungen)
                        {
                            nummer++;
                            if (itenErg.ja == erg.eigenes.ja && itenErg.nein == erg.eigenes.nein)
                            {
                                erg.rangplatz = nummer;
                            }
                        }
                    }

                    /*
                    //auf eine stelle runden
                    erg.gesamt.mittel = (float)Math.Round(erg.gesamt.mittel, 1, MidpointRounding.AwayFromZero);
                    erg.gesamt.strg = (float)Math.Round(erg.gesamt.strg, 1, MidpointRounding.AwayFromZero);
                    erg.gesamt.ja = (float)Math.Round(erg.gesamt.ja, 1, MidpointRounding.AwayFromZero);
                    erg.gesamt.nein = (float)Math.Round(erg.gesamt.nein, 1, MidpointRounding.AwayFromZero);
                    for (int n = 0; n < 5; n++)
                    {
                        erg.gesamt.skala[n] = (float)Math.Round(erg.gesamt.skala[n], 1, MidpointRounding.AwayFromZero);
                    }
                    erg.gesamt.minja = (float)Math.Round(erg.gesamt.minja, 1, MidpointRounding.AwayFromZero);
                    erg.gesamt.maxja = (float)Math.Round(erg.gesamt.maxja, 1, MidpointRounding.AwayFromZero);
                    erg.gesamt.minnein = (float)Math.Round(erg.gesamt.minnein, 1, MidpointRounding.AwayFromZero);
                    erg.gesamt.maxnein = (float)Math.Round(erg.gesamt.maxnein, 1, MidpointRounding.AwayFromZero);
                    if (erg.eigenes != null)
                    {
                        erg.eigenes.mittel = (float)Math.Round(erg.eigenes.mittel, 1, MidpointRounding.AwayFromZero);
                        erg.eigenes.strg = (float)Math.Round(erg.eigenes.strg, 1, MidpointRounding.AwayFromZero);
                        erg.eigenes.ja = (float)Math.Round(erg.eigenes.ja, 1, MidpointRounding.AwayFromZero);
                        erg.eigenes.nein = (float)Math.Round(erg.eigenes.nein, 1, MidpointRounding.AwayFromZero);
                        for (int n = 0; n < 5; n++)
                        {
                            erg.eigenes.skala[n] = (float)Math.Round(erg.eigenes.skala[n], 1, MidpointRounding.AwayFromZero);
                        }
                        erg.eigenes.minja = (float)Math.Round(erg.eigenes.minja, 1, MidpointRounding.AwayFromZero);
                        erg.eigenes.maxja = (float)Math.Round(erg.eigenes.maxja, 1, MidpointRounding.AwayFromZero);
                        erg.eigenes.minnein = (float)Math.Round(erg.eigenes.minnein, 1, MidpointRounding.AwayFromZero);
                        erg.eigenes.maxnein = (float)Math.Round(erg.eigenes.maxnein, 1, MidpointRounding.AwayFromZero);
                    }
                */
                }
            }
            catch (Exception ex) { 
                //dat.schreibeFehler("RacerDBNeu", ex.Message, ex);
            }

            double mean = -1;
            double ja = -1;
            double nein = -1;

            if (BMItem == 10) { mean = 3.587; ja = 55.5; nein = 14.2; }
            if (BMItem == 92) { mean = 3.39; ja = 48.3; nein = 20; }
            if (BMItem == 26) { mean = 3.599; ja = 57.9; nein = 15.6; }
            if (BMItem == 31) { mean = 3.397; ja = 49.8; nein = 21.3; }
            if (BMItem == 38) { mean = 3.35; ja = 49.9; nein = 22; }
            if (BMItem == 21) { mean = 3.741; ja = 64.8; nein = 9.7; }
            if (BMItem == 17) { mean = 3.531; ja = 54.4; nein = 15.4; }
            if (BMItem == 20) { mean = 3.777; ja = 63.4; nein = 10.4; }
            if (BMItem == 65) { mean = 3.6; ja = 57.2; nein = 12.2; }
            if (BMItem == 44) { mean = 3.898; ja = 68.7; nein = 10.3; }
            if (BMItem == 22) { mean = 3.745; ja = 63.7; nein = 10.9; }
            if (BMItem == 72) { mean = 3.563; ja = 56; nein = 14.1; }
            if (BMItem == 13) { mean = 3.635; ja = 60.1; nein = 13.5; }
            if (BMItem == 34) { mean = 3.41; ja = 51.3; nein = 20.4; }
            if (BMItem == 4) { mean = 3.607; ja = 55.7; nein = 13.1; }
            if (BMItem == 2) { mean = 4.082; ja = 77.8; nein = 6.2; }
            if (BMItem == 5) { mean = 3.472; ja = 52; nein = 19.2; }
            if (BMItem == 7) { mean = 3.801; ja = 65.9; nein = 10.9; }
            if (BMItem == 11) { mean = 3.719; ja = 63.4; nein = 12.1; }
            if (BMItem == 15) { mean = 3.848; ja = 68.1; nein = 10.2; }
            if (BMItem == 18) { mean = 3.853; ja = 68.6; nein = 10.3; }
            if (BMItem == 49) { mean = 3.609; ja = 58.4; nein = 12.6; }
            if (BMItem == 50) { mean = 3.839; ja = 67.2; nein = 10.7; }
            if (BMItem == 53) { mean = 3.287; ja = 46.4; nein = 25.6; }
            if (BMItem == 60) { mean = 3.735; ja = 61.8; nein = 10.5; }
            if (BMItem == 62) { mean = 3.461; ja = 52.4; nein = 18.3; }
            if (BMItem == 67) { mean = 3.612; ja = 58.3; nein = 15.5; }
            if (BMItem == 76) { mean = 3.516; ja = 52.5; nein = 15.4; }
            if (BMItem == 77) { mean = 3.478; ja = 52.6; nein = 18.1; }
            if (BMItem == 79) { mean = 3.699; ja = 61; nein = 12.7; }
            if (BMItem == 84) { mean = 3.518; ja = 53.2; nein = 16.9; }
            if (BMItem == 88) { mean = 3.941; ja = 71.6; nein = 8.4; }


            if (erg != null)
            {
                if (mean < 0)
                {
                    erg.benchmark = null;
                }
                else
                {
                    erg.benchmark = new ItemErg();
                    erg.benchmark.anzahl = 1;
                    erg.benchmark.mittel = (float)mean;
                    erg.benchmark.ja = (float)ja;
                    erg.benchmark.nein = (float)nein;
                }
            }
            return erg;
        }

        private static int compareErg(ItemErg i1, ItemErg i2)
        {
            if (i1.ja > i2.ja)
            {
                return -1;
            }
            if (i1.ja < i2.ja)
            {
                return 1;
            }
            if (i1.ja == i2.ja)
            {
                if (i1.nein < i2.nein)
                {
                    return -1;
                }
                if (i1.nein > i2.nein)
                {
                    return 1;
                }
            }
            return i1.befragung.CompareTo(i2.befragung);
        }

       
        public string toFilter(string gewählte)
        {
            //gewählte ist: merkmal + "#" + kat + "$"
            string erg = "";
            string[] teile = gewählte.Split(new char[] { '$' });
            List<int>[] gewählteKategorie = new List<int>[15];
            for (int n = 0; n < 15; n++)
            {
                gewählteKategorie[n] = new List<int>();
            }
            foreach (string teil in teile)
            {
                if (teil != "")
                {
                    string[] merkmalkat = teil.Split(new char[] { '#' });
                    if (merkmalkat.Length == 2)
                    {
                        int merkmal = int.Parse(merkmalkat[0]);
                        int kat = int.Parse(merkmalkat[1]);
                        gewählteKategorie[merkmal - 1].Add(kat);
                    }
                }
            }
            List<string> einzelfilter = new List<string>();
            einzelfilter.Add("");
            for (int n = 0; n < 15; n++)
            {
                if (gewählteKategorie[n].Count == 0)
                {
                    for (int i = 0; i < einzelfilter.Count; i++)
                    {
                        einzelfilter[i] = einzelfilter[i] + "0#";
                    }
                }
                else
                {
                    List<string> tempEinzelFilter = new List<string>();
                    for (int i = 0; i < einzelfilter.Count; i++)
                    {
                        foreach (int kat in gewählteKategorie[n])
                        {
                            tempEinzelFilter.Add(einzelfilter[i] + kat.ToString() + "#");
                        }
                    }
                    einzelfilter.Clear();
                    foreach (string s in tempEinzelFilter)
                    {
                        einzelfilter.Add(s);
                    }
                }
            }

            erg = "(";
            foreach (string s in einzelfilter)
            {
                if (erg != "(")
                {
                    erg = erg + " OR ";
                }
                erg = erg + "[stats]='" + s + "'";
            }
            erg = erg + ")";
            return erg;
        }

        /// <summary>
        /// liefert das Ergebnis einer befragung zu einem Item, Index oder subindex
        /// </summary>
        /// <param name="befragung"></param>
        /// <param name="stat"></param>
        /// <param name="BMIndex"></param>
        /// <param name="BMSubIndex"></param>
        /// <param name="BMItem"></param>
        /// <param name="wsUser"></param>
        /// <param name="wsPass"></param>
        /// <returns></returns>
       
        public ItemErg getBenchmarkItemBefragungErg(DataTable dat, int befragung, string filter, int BMIndex, int BMSubIndex, int BMItem, int cutoff)
        {
            ItemErg erg = new ItemErg();

            erg.mittel = 0;
            erg.strg = 0;
            erg.ja = 0;
            erg.nein = 0;
            erg.anzahl = 0;
            erg.skala = new double[5];
            erg.skala[0] = 0;
            erg.skala[1] = 0;
            erg.skala[2] = 0;
            erg.skala[3] = 0;
            erg.skala[4] = 0;
            erg.maxja = 0;
            erg.minja = 0;
            erg.maxnein = 0;
            erg.minnein = 0;

            string filterselect = "";
            filterselect = filterselect + "BefragungID=" + befragung + " AND ";
            filterselect = filterselect + "BMIndex=" + BMIndex + " AND ";
            filterselect = filterselect + "BMSubIndex=" + BMSubIndex + " AND ";
            filterselect = filterselect + "BMItem=" + BMItem;
            string q;

                q = "SELECT , Strg, Ja, Nein, Anz, P1, P2, P3, P4, P5 FROM Ergebnis WHERE ";
                q = q + filterselect;

            DataRow[] result = dat.Select(filterselect);

                foreach(DataRow row in result)
                {
                    int anz = (int)row["Anz"];
                    erg.anzahl += anz;
                    erg.mittel += (double)row["Mittelwert"] * anz;
                    erg.strg += (double)row["Strg"] * anz;
                    erg.ja += (double)row["Ja"] * anz;
                    erg.nein += (double)row["Nein"] * anz;
                    erg.skala[0] += (double)row["P1"] * anz;
                    erg.skala[1] += (double)row["P2"] * anz;
                    erg.skala[2] += (double)row["P3"] * anz;
                    erg.skala[3] += (double)row["P4"] * anz;
                    erg.skala[4] += (double)row["P5"] * anz;
                    erg.maxja += (double)row["Ja"] * anz;
                    erg.minja += (double)row["Ja"] * anz;
                    erg.maxnein += (double)row["Nein"] * anz;
                    erg.minnein += (double)row["Nein"] * anz;
                }

                if (erg.anzahl >= cutoff)
                {
                    erg.mittel = erg.mittel / erg.anzahl;
                    erg.strg = erg.strg / erg.anzahl; ;
                    erg.ja = erg.ja / erg.anzahl; ;
                    erg.nein = erg.nein / erg.anzahl; ;
                    erg.skala[0] = erg.skala[0] / erg.anzahl; ;
                    erg.skala[1] = erg.skala[1] / erg.anzahl; ;
                    erg.skala[2] = erg.skala[2] / erg.anzahl; ;
                    erg.skala[3] = erg.skala[3] / erg.anzahl; ;
                    erg.skala[4] = erg.skala[4] / erg.anzahl; ;
                    erg.maxja = erg.maxja / erg.anzahl; ;
                    erg.minja = erg.minja / erg.anzahl; ;
                    erg.maxnein = erg.maxnein / erg.anzahl; ;
                    erg.minnein = erg.minnein / erg.anzahl; ;
                }
                else
                {
                    erg.mittel = 0;
                    erg.strg = 0;
                    erg.ja = 0;
                    erg.nein = 0;
                    erg.anzahl = 0;
                    erg.skala = new double[5];
                    erg.skala[0] = 0;
                    erg.skala[1] = 0;
                    erg.skala[2] = 0;
                    erg.skala[3] = 0;
                    erg.skala[4] = 0;
                    erg.maxja = 0;
                    erg.minja = 0;
                    erg.maxnein = 0;
                    erg.minnein = 0;
                }
        
            return erg;
        }

        public class MappingIndex
        {
            public int id;
            public string code;
            public string textD;
            public string textE;
            public List<MappingSubIndex> subindizes = new List<MappingSubIndex>();
            public List<int> items = new List<int>();
        }

        public class MappingSubIndex
        {
            public int id;
            public string code;
            public string textD;
            public string textE;
            public List<int> items = new List<int>();
        }

        /// <summary>
        /// 
        /// </summary>
        public class BMErg
        {
            public Unternehmen unternehmen;
            public List<BMItemIndex> items=new List<BMItemIndex>();
            public List<BMGruppenErg> gruppenErgs=new List<BMGruppenErg>();
            public bool valid = false;
            public int deleted = -1;
            public int edited = -1;
            public bool isGroupEditing = false;
        }


        public class BMGruppenErg
        {
            public int nummer;
            public int aktualisierung=-1;
            public int year=-1;
            public List<Unternehmen> unternehmen;
            public string titel;
            public string filter;
            public bool collapsed = true;
            public bool wasSaved = true;
            public List<BMItemErg> itemErgs;
            public async Task rechne(WSRacer wsRacer, int eigenesUnternehmenID, List<BMItemIndex> items, int companyCutOff)
            {
                List<int> befragungen=null;
                if (this.aktualisierung>=0)
                {
                    befragungen = wsRacer.getAlleBefragungenIDsInAktualisierung(this.aktualisierung);
                }
                if (this.year>0)
                {
                    befragungen = wsRacer.getAlleBefragungenIDsInJahr(this.year);
                }

                int minanz = companyCutOff;
                int cutoff = 15;
                this.itemErgs =await wsRacer.getGesamtBenchmark(eigenesUnternehmenID, this.aktualisierung,befragungen.ToArray(), items, filter,minanz, cutoff);
            }

            public void getTitelFromFilter(string sprache, WSRacer wsRacer)
            {
                if (filter == "")
                {
                    titel = "RACER";
                    return;
                }

                titel = "";
                string[] filterParts = filter.Split('$');
                foreach (string filterPart in filterParts)
                {
                    string[] merkmalkat = filterPart.Split('#');
                    int merkmal = int.Parse(merkmalkat[0]);
                    int kat = int.Parse(merkmalkat[1]);
                    string katText = wsRacer.getKatText(kat, merkmal, sprache);
                    if (titel != "") { titel = titel + ", "; }
                    titel = titel + katText;
                }

            }
        }

        public class BMItemErg
        {
            public bool gültig = true;
            public ItemErg gesamt;
            public ItemErg eigenes;
            public ItemErg benchmark;
            public List<ItemErg> alleBefragungen;
            public int rangplatz;
        }

        public class ItemErg
        {
            public int befragung;
            public Unternehmen unternehmen;
            public double ja = 0;
            public double nein = 0;
            public double mittel = 0;
            public double strg = 0;
            public int anzahl = 0;
            public double[] skala = new double[] { 0, 0, 0, 0, 0 };
            public double minmittel = 5;
            public double maxmittel = 1;
            public double minja = 100;
            public double maxja = 0;
            public double minnein = 100;
            public double maxnein = 0;
            public int anzBefragungen = 1;
        }

        public class BMItemIndex
        {
            public int id;
            public string code;
            public string text;
            public string typ;
        }

        public class Unternehmen
        {
            public int id;
            public string name;
            public int nachkommastellen;
            public int companyCutOff;
        }

        public class Befragung
        {
            public bool inNeuerAktualisierung;
            public int id;
            public string datum;
            public int teilnehmer;
            public bool vollbefragung;
            public int unternehmenID;
            public string unternehmenName;
            public int aktualisierungsnummer;
            public string rücklauf;
            public string cycle;
            public int cutOff;
            public string quotePaper;
            public string quoteOnline;
        }

        public class AnzeigeDemoMerkmal
        {
            public int id;
            public string textd;
            public string texte;
        }

        public class FilterDemoMerkmal
        {
            public int id;
            public int anzeigeMerkmal;
            public string kürzel;
        }

        public class DemoKategorie
        {
            public int id;
            public string textd;
            public string texte;
            /// <summary>
            /// für z.B. Region, da wird nur Region gezeigt, es sind aber eigentlich 3 Variavblen
            /// </summary>
            public int anzeigemerkmal;
            /// <summary>
            /// für dei einzelnen level
            /// </summary>
            public int filtermerkmal;
            public int anzNennungen;
            public int level;
        }

        public class Aktualisierung
        {
            public int id;
            public string datum;
            //public bool altesModell;
        }

        public class Skala
        {
            public int min;
            public int max;
        }
    }
}