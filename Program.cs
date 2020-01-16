using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
namespace Migration1
{
    class Program
    {
        public static int g_bury_seq = 1;//묘지 계약 순번 
        public static int g_dead_seq = 1;// 고인순번 
        public static int g_ens_seq = 1; //봉안 계약 순번 
        public static int g_trans_rec = 0; // 처리된 레코드 
        public static int g_ext_no = 0;
        public static int g_nature_seq = 1; //자연장 계약 순번
        public static int g_appl_seq = 1;// 계약자 순번 
                                         // 이전디비접속 정보 
        public static SqlConnection sconorg;
        public static SqlConnection scondst;
        public static SqlCommand scomorg;
        public static SqlConnection sconorg2;
        public static SqlCommand scomorg2;
        public static SqlCommand scomorg3;
        public static SqlCommand scomdst;
        public static SqlCommand scomdst2;
        public static SqlCommand scomdst3;
        DateTime StartTime, EndTime;

        public  static LogWriter ResultLog = new LogWriter("mitration");

        static String get_APP_ID(String name,String tel1,String tel2, String tel3,String post)
        {
            String app_id = "";
            scomdst.CommandText = @"SELECT MAX([APPL_ID])  FROM [funeralsystem_dangjin].[dbo].[applicant]
                                   where [APPL_NAME]='" + name + "' and [TELNO1]='" + tel1 + "' and [TELNO2]='" + tel2 + "' and [TELNO3]='" + tel3 + "' and [APPL_POST]='" + post+"'";

            object re_object = scomdst.ExecuteScalar();
            if (re_object != null)
                return re_object.ToString();
            else
                return "-1";

        }
        static String get_DEAD_ID(String name, String age, String deaddate)
        {
            String app_id = "";
            if (String.IsNullOrEmpty(deaddate))
            scomdst.CommandText = @"SELECT MAX([DEAD_ID])  FROM [funeralsystem_dangjin].[dbo].[thedead]
                                   where [DEAD_NAME]='" + name + "' and [DEAD_AGE]=isnull('" + age + "',0) and [DEAD_DATE] IS NULL";
            else
                scomdst.CommandText = @"SELECT MAX([DEAD_ID])  FROM [funeralsystem_dangjin].[dbo].[thedead]
                                   where [DEAD_NAME]='" + name + "' and [DEAD_AGE]=isnull('" + age + "',0) and [DEAD_DATE]=LEFT('" + deaddate + "',10)";

            object re_object = scomdst.ExecuteScalar();
            if (re_object != null)
                return re_object.ToString();
            else
                return "-1";

        }
        static void Main(string[] args)
        {

            Console.WriteLine("connect 1");
            string connectionstringorg = "server=108.15.0.108;uid=mgadmin;pwd=laputa9;database=dangjin;";//실서버
            //string connectionstringorg = "server=WIN-FVLD4RP0BJF;Integrated Security=SSPI;database=dangjin;";//복제서버 
            Console.WriteLine("기존서버연결 .......OK");
            sconorg = new SqlConnection(connectionstringorg);
            scomorg = new SqlCommand();
            scomorg.Connection = sconorg;
            scomorg.CommandText = "SELECT TOP (100) seq,juminb FROM [dangjin].[dbo].[tb_bong]";
            sconorg.Open();
            sconorg2 = new SqlConnection(connectionstringorg);
            scomorg2 = new SqlCommand();
            scomorg2.Connection = sconorg2;
            sconorg2.Open();
            scomorg3 = new SqlCommand();
            scomorg3.Connection = sconorg2;
            //새디비접속 정보 g
            Console.WriteLine("connect 2");
            string connectionstringdst = "server=108.15.0.211;Initial Catalog=funeralsystem_dangjin;uid=mgadmin;pwd=laputa9;database=funeralsystem_dangjin;";//실서버 
            //string connectionstringdst = "server =WIN-FVLD4RP0BJF;Integrated Security = SSPI;database =funeralsystem_dangjin;";//복제서버
            Console.WriteLine("신서버연결 .......OK");

            scondst = new SqlConnection(connectionstringdst);
            scomdst = new SqlCommand();
            scomdst.Connection = scondst;
            scomdst2 = new SqlCommand();
            scomdst2.Connection = scondst;
            scomdst3 = new SqlCommand();
            scomdst3.Connection = scondst;
            scomdst.CommandText = "SELECT TOP (100) [DEAD_ID] ,[DEAD_NAME] FROM [funeralsystem_dangjin].[dbo].[thedead]";
            scondst.Open();
            //   SqlDataReader sdrdst = scomdst.ExecuteReader();
            SqlDataReader sdrorg = scomorg.ExecuteReader();
            SqlDataReader sdrorg2, sdrdst2, sdrorg3;

            /*-------------------------------------------------------------------------
             * 1.고인정보  (tb_bong_damo) ->  tWthedead_damo 
                -------------------------------------------------
                -- 봉안자정보
                -------------------------------------------------
                CREATE TABLE [dbo].[tb_bong_damo](
	                [seq]        [int] IDENTITY(1,1) NOT NULL,  -- 연번
	                [seq_myoji]  [int] NOT NULL,                -- 묘지키(묘지테이블의 SEQ)
	                [unameb]     [varchar](50) NOT NULL,        -- 고인명  
	                [mdate]      [varchar](10) NOT NULL,        -- 매장일자 
	                [sec_juminb] [varbinary](64) NOT NULL,      -- 주민번호
	                [sleepdate]  [varchar](10) NOT NULL,        -- 사망일자 
	                [addrb]      [varchar](200) NOT NULL,       -- 주소  
	                [sleepwhy]   [varchar](50) NOT NULL,        -- 사망사유
	                [sleepon]    [varchar](50) NOT NULL,        -- 사망장소 
	                [age]        [varchar](3) NULL,             -- 사망당시나이 
	                [btype]      [int] NOT NULL,                -- 매장구분  (0:시매,1:화매,2:이매,3:기타,4:예약)  
	                [related2]   [varchar](20) NOT NULL,        -- 계약자와관계
	                [istop]      [tinyint] NULL,                -- 대표고인여부(1:대표고인,0:대표아님)   
                 CONSTRAINT [PK_tb_bong_D1] PRIMARY KEY CLUSTERED 
	                [seq] ASC
	        ------------------------------------------------
            -- 고인정보 --
            ------------------------------------------------
            DROP TABLE  tWthedead_damo;
            CREATE TABLE tWthedead_damo (                                                            
            DEAD_ID           INT IDENTITY(1,1) NOT NULL , -- COMMENT '고인번호',                 

            DEAD_NAME         NVARCHAR(100) DEFAULT NULL , -- COMMENT '고인명',                        
            sec_DEAD_JUMIN        NVARCHAR(24)  DEFAULT NULL , -- COMMENT '사망자주민번호',            
            DEAD_SEX          NVARCHAR(10)  DEFAULT NULL , -- COMMENT '고인성별',      -- TCM05                    
            DEAD_AGE          INT           DEFAULT NULL , -- COMMENT '사망당시나이',                       
            DEAD_DATE         DATE          DEFAULT NULL , -- COMMENT '사망일자',                             
            DEAD_PLACE        NVARCHAR(10)  DEFAULT NULL , -- COMMENT '사망장소',       -- TCM09                  
            DEAD_PLACE_TXT    NVARCHAR(10)  DEFAULT NULL , -- COMMENT '사망장소상세',                     
            DEAD_REASON       NVARCHAR(10)  DEFAULT NULL , -- COMMENT '사망사유',       -- TCM03                 
            DEAD_REASON_TXT   NVARCHAR(100) DEFAULT NULL , -- COMMENT '사망사유상세',
            OBJT              NVARCHAR(10)  DEFAULT NULL , -- COMMENT '안치대상자구분', -- TFM01                                           

            ADDR_GUBUN        NVARCHAR(10)  DEFAULT NULL , -- COMMENT '주소구분',       -- TCM07       
            DEAD_POST         NVARCHAR(6)   DEFAULT NULL , -- COMMENT '고인우편번호',                 
            DEAD_ADDR1        NVARCHAR(200) DEFAULT NULL , -- COMMENT '고인주소1',                   
            DEAD_ADDR2        NVARCHAR(200) DEFAULT NULL , -- COMMENT '고인주소2',                   
            DEAD_REMARK       NVARCHAR(200) DEFAULT NULL , -- COMMENT '고인비고',                   
            REGTIME           DATETIME NULL DEFAULT NULL , -- COMMENT '등록시간',                     
            REGID             NVARCHAR(20)  DEFAULT NULL , -- COMMENT '등록자ID',                           
            UDTTIME           DATETIME NULL DEFAULT NULL , -- COMMENT '수정시간',                     
            UDTID             NVARCHAR(20)  DEFAULT NULL , -- COMMENT '수정자ID',                           
            CONSTRAINT [PK_tWthedead_damo] PRIMARY KEY CLUSTERED 
            (
	            DEAD_ID
            ))                
             */
            /*  Console.WriteLine("tWthedead_damo생성시작");
              scomorg.CommandText = "SELECT TOP (1000) seq ,seq_myoji ,[unameb] ,[mdate],[sec_juminb],[sleepdate] ,[addrb],[sleepwhy],[sleepon],[age],[btype],[related2],[istop] FROM [dangjin].[dbo].[tb_bong_damo]";
              //sconorg.Open();
              sdrorg.Close();
              //sdrdst.Close();
              sdrorg = scomorg.ExecuteReader();
               while (sdrorg.Read())
                {
                    Console.WriteLine(" seq :: " + sdrorg["seq"].ToString());
                    Console.WriteLine(" seq_myoji :: " + sdrorg["seq_myoji"].ToString());
                } */

            /*----------------------------------------------------------------------------------------------*/
            //3.묘지정보  (tb_myoji)   lll
            //-----------------------------------------------------------------------------------------------/
            //tb_myoji 읽어들임
            Console.WriteLine("tb_myoji 이전시작");
            Console.WriteLine("Press 'C' key to start");
            ConsoleKeyInfo result = Console.ReadKey(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            // new DB 초기화 
            scomdst.CommandText = @"truncate table buryext;
                                    truncate table bury;
                                    truncate table enshrine;
                                    truncate table ensext;
                                    truncate table nature;
                                    truncate table burydead;
                                    truncate table ensdead;
                                    truncate table naturedead;
            ";

            scomdst.ExecuteNonQuery();

            scomorg.CommandText = @"SELECT  [seq]
      ,[mno]
      ,[gtype]
      ,[mtype]
      ,[mptype]
      ,[expdate]
      ,[expnum]
      ,[ydate]
      ,[ysize]
      ,[asize]
      ,[setdate]
      ,[cost]
      ,[munit]
      ,[mnotnum]
      ,[mnotcost]
      ,[mnotpsdate]
      ,[mnotpedate]
      ,[ucost]
      ,[sumcost]
      ,[usecost]
      ,[outwhy]
      ,[outdate]
      ,[madecomp]
      ,[maxkisu]
      ,[bongtype]
      ,[appno]
      ,[acceptno]
      ,[seq_customer]
      ,[seq_setupman]
      ,[seq_outman]
      ,[jancost]
      ,[etc]
      ,[old_mno]
  FROM [dangjin].[dbo].[tb_myoji]  ";
            //sconorg.Open();
            sdrorg.Close();
            //sdrdst.Close();
            ResultLog.LogWrite("전체 묘지 DB select");
            sdrorg = scomorg.ExecuteReader();
            g_bury_seq = 1;
            g_dead_seq = 1;
            g_ens_seq = 1;
            g_nature_seq = 1;
           
            while (sdrorg.Read())
            {
                string org_mno = sdrorg["mno"].ToString();
                if (org_mno.Contains("나"))
                {
                    org_mno =org_mno.Replace("--", "-");
                }
                if (org_mno.Equals("02-00-00-0017-00-3"))
                {
                    org_mno = org_mno;
                }
                string[] mno_code = org_mno.ToString().Split('-'); //묘번 파싱
                
                string fac_id, bury_type, bury_kind, loc_code, dan_no, use_gubun, dc_gubun, made_comp, remark, reg_id, udt_id, mno;
                DateTime bury_date, set_date, str_date, end_date, ext_str_date, ext_end_date, receipt_date, reg_time, udttime;
                int row_no, col_no, appl_no, ext_cnt, use_ori_amt, use_dc_amt, use_amt, mgmt_amt, glass_amt, bury_no, bury_no2, span, span2;
                if (mno_code.Length < 5) continue;
                loc_code = mno_code[1];
                dan_no = mno_code[2];

                // mno의 마지막 항목에 따라 1:분묘단장, 2:분묘합장,5:개인봉안묘단장, 6:개인봉안묘합장, 7:가족납골묘 는 bury(묘지계약정보)가 이관대상
                int tbury_num;

                switch (mno_code[5])
                {
                    case "1":
                    case "2":
                    case "5":
                    case "6":
                    case "7":
                    case "9":
                        //bury 이전 묘지
                        // 묘지번호 구성에 따라 단, 행,열의 정보 추출
                        //연번 
                        //                        g_bury_seq++;
                        try
                        {
                            bury_no = Int32.Parse(mno_code[3]);//단열년번 -bury
                        }
                        catch (Exception ex)
                        {
                            bury_no = 0; 
                        }
                        try
                        {
                            bury_no2 = Int32.Parse(mno_code[4]);//부번 - bury
                        }
                        catch (Exception ex)
                          {
                            bury_no2 = 0;
                        }
                        fac_id = "04";
                        bury_type = "TFM0400001";
                        bury_kind = "TFM0500001";
                        receipt_date = DateTime.Now;
                        switch (mno_code[0])
                        {
                            case "01": //석문 
                                {
                                    fac_id = "01";
                                    switch (mno_code[5])
                                    {
                                        case "1":
                                            bury_type = "TFM0400001";// 개인묘지
                                            bury_kind = "TFM0500001";
                                            break;
                                        case "7":
                                            bury_type = "TFM0400004";
                                            bury_kind = "TFM0500002";
                                            break;
                                        case "9":
                                            bury_type = "TFM0400003";// 행려분묘=>봉안평장
                                            bury_kind = "TFM0500001";
                                            break;
                                    }
                                    switch (mno_code[1])
                                    {
                                        case "가":
                                            loc_code = "01";
                                            break;
                                        case "나":
                                            loc_code = "02";
                                            break;
                                        default:
                                            loc_code = mno_code[1];
                                            break;

                                    }
                                    dan_no = mno_code[2];
                                    switch (mno_code[2])// 단번호 처리 
                                    {
                                        case "01":
                                            dan_no = mno_code[2];
                                            switch (mno_code[3])
                                            {
                                                case "0001":
                                                    row_no = 3;
                                                    col_no = 1;
                                                    break;
                                                case "0002":
                                                    row_no = 2;
                                                    col_no = 2;
                                                    break;
                                                case "0003":
                                                    row_no = 3;
                                                    col_no = 2;
                                                    break;
                                                case "0004":
                                                    row_no = 2;
                                                    col_no = 3;
                                                    break;
                                                case "0005":
                                                    row_no = 3;
                                                    col_no = 3;
                                                    break;
                                                case "0006":
                                                case "0007":
                                                case "0008":
                                                case "0009":
                                                case "0010":
                                                case "0011":
                                                    tbury_num = Int32.Parse(mno_code[3]);
                                                    row_no = tbury_num % 3 + 1;
                                                    col_no = (tbury_num / 3) + 2;
                                                    break;
                                            }
                                            break;
                                        case "02":
                                        case "03":
                                        case "04":
                                            dan_no = mno_code[2];

                                            break;
                                    }
                                    break;
                                }
                            case "02": //대호지
                                fac_id = "02";
                                switch (mno_code[5]) //안치형태
                                {
                                    case "1":
                                        bury_type = "TFM0400001";// 분묘
                                        bury_kind = "TFM0500001";
                                        break;
                                    case "5":
                                        bury_type = "TFM0400001";// 개인봉안
                                        bury_kind = "TFM0500001";
                                        break;
                                    case "6":
                                        bury_type = "TFM0400006";// 개인봉안
                                        bury_kind = "TFM0500002";
                                        break;
                                    case "7":
                                        bury_type = "TFM0400004";
                                        bury_kind = "TFM0500002";
                                        break;
                                    case "9":
                                        bury_type = "TFM0400003";// 행려분묘=>봉안평장
                                        bury_kind = "TFM0500001";
                                        break;
                                }
                                switch (mno_code[1]) //구역번호 처리 
                                {
                                    case "가":
                                        loc_code = "01";
                                        break;
                                    case "나":
                                        loc_code = "02";
                                        break;
                                    default:
                                        loc_code = mno_code[1];
                                        break;

                                }
                                //단번호 처리
                                if (mno_code[3].Equals(""))
                                {
                                    dan_no = mno_code[3];
                                    //bury_no = mno_code[4];
                                }
                                else
                                    dan_no = mno_code[2];
                                //tbury_num = Int32.Parse(bury_no);
                                tbury_num = 1;

                                break;
                            case "03": //솔뫼 
                                fac_id = "03";
                                switch (mno_code[5]) //안치형태
                                {
                                    case "1":
                                        bury_type = "TFM0400001";// 분묘
                                        bury_kind = "TFM0500001";
                                        break;
                                    case "5":
                                        bury_type = "TFM0400001";// 개인봉안 
                                        bury_kind = "TFM0500001";
                                        break;
                                    case "7":
                                        bury_type = "TFM0400004";// 가족봉안
                                        bury_kind = "TFM0500002";
                                        break;
                                    case "9":
                                        bury_type = "TFM0400003";// 행려분묘=>봉안평장
                                        bury_kind = "TFM0500001";
                                        break;
                                }
                                switch (mno_code[1]) //구역번호 처리 
                                {
                                    case "가":
                                        loc_code = "01";
                                        break;
                                    case "나":
                                        loc_code = "02";
                                        break;
                                    default:
                                        loc_code = mno_code[1];
                                        break;

                                }
                                dan_no = mno_code[2];

                                break;
                            case "04": //남부 
                                fac_id = "04";
                                switch (mno_code[5]) //안치형태
                                {
                                    case "1":
                                        bury_type = "TFM0400001";// 분묘
                                        bury_kind = "TFM0500001";
                                        break;
                                    case "5":
                                        bury_type = "TFM0400001";// 개인봉안
                                        bury_kind = "TFM0500001";
                                        break;
                                    case "8":
                                        bury_type = "TFM0400008";// 자연장
                                        bury_kind = "TFM0500001";
                                        break;
                                    case "9":
                                        bury_type = "TFM0400003";// 행려분묘=>봉안평장
                                        bury_kind = "TFM0500001";
                                        break;
                                }
                                switch (mno_code[1]) //구역번호 처리 
                                {
                                    case "가":
                                        loc_code = "01";
                                        break;
                                    case "나":
                                        loc_code = "02";
                                        break;
                                    default:
                                        loc_code = mno_code[1];
                                        break;

                                }
                                dan_no = mno_code[2];
                                break;
                        }
                        //행,열 번호 초기화 
                        row_no = 0;
                        col_no = 0;
                        span = 0;
                        // 안치형태 
                        //bury_type;
                        //합장구분
                        //bury_kind
                        //계약일자 -bury
                        try
                        {
                            bury_date = DateTime.Parse(sdrorg["ydate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            bury_date = DateTime.Now;
                        }
                        //설치일자 -bury
                        try
                        {
                            set_date = DateTime.Parse(sdrorg["setdate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            set_date = DateTime.Now;
                        }
                        //만기일자 -bury
                        try
                        {
                            end_date = DateTime.Parse(sdrorg["expdate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            end_date = set_date;// DateTime.Now;
                        }
                        //안치형태 - 매장
                        String mptype = "";
                        mptype = sdrorg["mtype"].ToString();
                        switch (mptype)
                        {
                            case "0"://분묘단장
                                bury_type = "TFM0400001";//분묘
                                bury_kind = "TFM0500001";//단장
                                break;
                            case "1"://분묘합장
                                bury_type = "TFM0400001";//분묘
                                bury_kind = "TFM0500002";//합장
                                break;
                            case "2"://납골개인
                                bury_type = "TFM0400001";//분묘
                                bury_kind = "TFM0500001";//단장
                                break;
                            case "3"://납골무연
                                bury_type = "TFM0400001";//분묘
                                bury_kind = "TFM0500001";//단장
                                break;
                            case "4"://개인봉안묘단장
                                bury_type = "TFM0400006";//개인봉안묘
                                bury_kind = "TFM0500001";//단장
                                break;
                            case "5"://개인봉안묘합장
                                bury_type = "TFM0400006";//개인봉안묘
                                bury_kind = "TFM0500002";//합장
                                break;
                            case "6"://가족납골묘
                                bury_type = "TFM0400004";//가족묘지
                                bury_kind = "TFM0500002";//합장
                                break;
                            case "7": //자연장
                                bury_type = "TFM0400001";//분묘
                                bury_kind = "TFM0500001";//단장
                                break;
                            case "8"://기타
                                bury_type = "TFM0400001";//분묘
                                bury_kind = "TFM0500001";//단장
                                break;
                            case "9": //납골부부
                                bury_type = "TFM0400001";//가족묘지
                                bury_kind = "TFM0500001";//합장
                                break;
                        }
                        //솔뫼의 경우 가족봉안묘 16->24 

                        if (fac_id.Equals("03") && bury_type.Equals("TFM0400004")) bury_type = "TFM0400005";
                        // 끝자리가 9면 형례분묘
                        if (mno_code[5].Equals("9"))
                        {
                            bury_type = "TFM0400003";// 행려분묘=>봉안평장
                            bury_kind = "TFM0500001";
                        }
                            //계약기간 계산 
                            span = end_date.Year - bury_date.Year;
                        ext_cnt = 0;
                        scomorg2.Parameters.Clear();
                        // tb_cust_damo 에서 계약자 정보 꺼냄 -bury
                        scomorg2.CommandText = @"SELECT TOP (1000) [seq]
                                                  ,[unamea]
                                                  ,[jumin]
                                                  ,[tel]
                                                  ,[hp]
                                                  ,[email]
                                                  ,[zip]
                                                  ,[addr]
                                                  ,[related]
                                              FROM [dangjin].[dbo].[tb_cust]
                                              where [seq]=@seq_cust";
                        SqlParameter param = new SqlParameter();
                        param.ParameterName = "@seq_cust";
                        param.Value = sdrorg["seq_customer"];
                        scomorg2.Parameters.Add(param);
                        sdrorg2 = scomorg2.ExecuteReader();
                        g_appl_seq = 0;
                        //계약자 정보 이전  -bury 
                        while (sdrorg2.Read())
                        {
                            //g_appl_seq++;
                            scomdst.CommandText = @"INSERT INTO [dbo].[applicant]
                                                   ([APPL_NAME]
                                                   ,[APPL_JUMIN]
                                                   ,[TELNO1]
                                                   ,[TELNO2]
                                                   ,[TELNO3]
                                                   ,[MOBILENO1]
                                                   ,[MOBILENO2]
                                                   ,[MOBILENO3]
                                                   ,[ADDR_GUBUN]
                                                   ,[APPL_POST]
                                                   ,[APPL_ADDR1]
                                                   ,[APPL_ADDR2]
                                                   ,[APPL_REMARK])
                                             VALUES
                                                   (@APPL_NAME
                                                   ,@APPL_JUMIN
                                                   ,@TELNO1
                                                   ,@TELNO2
                                                   ,@TELNO3
                                                   ,@MOBILENO1
                                                   ,@MOBILENO2
                                                   ,@MOBILENO3
                                                   ,@ADDR_GUBUN
                                                   ,@APPL_POST
                                                   ,@APPL_ADDR1
                                                   ,'   ','   ')";

                            string[] tel_no = sdrorg2["tel"].ToString().Split('-');
                            string[] hp_no = sdrorg2["hp"].ToString().Split('-');
                            //SqlParameter param0 = new SqlParameter();
                            //param0.ParameterName = "@APPL_ID";
                            //param0.Value = g_appl_seq;
                            //scomdst.Parameters.Add(param0);
                            SqlParameter param1 = new SqlParameter();
                            param1.ParameterName = "@APPL_NAME";
                            param1.Value = sdrorg2["unamea"];
                            scomdst.Parameters.Add(param1);
                            SqlParameter param2 = new SqlParameter("@APPL_JUMIN", SqlDbType.VarChar,14);
                            param2.Value = sdrorg2["jumin"];
                            scomdst.Parameters.Add(param2);
                            SqlParameter param3 = new SqlParameter();
                            param3.ParameterName = "@TELNO1";
                            if (tel_no.Length < 3)
                                param3.Value = "041";
                            else
                                param3.Value = tel_no[0];
                            SqlParameter param3_2 = new SqlParameter();
                            param3_2.ParameterName = "@TELNO2";
                            if (tel_no.Length < 3)
                                param3_2.Value = tel_no[0];
                            else
                                param3_2.Value = tel_no[1];
                            scomdst.Parameters.Add(param3_2);
                            SqlParameter param3_3 = new SqlParameter();
                            param3_3.ParameterName = "@TELNO3";
                            if (tel_no.Length > 1)
                            {
                                if (tel_no.Length < 3)
                                    param3_3.Value = tel_no[0];
                                else
                                    param3_3.Value = tel_no[2];
                            }
                            else param3_3.Value = "";
                            if (tel_no.Length <= 1)
                            {
                                param3.Value = "";
                                param3_2.Value = "";
                                param3_3.Value = "";

                            }
                            if (param3_2.Value.Equals("")) param3.Value = "";
                            scomdst.Parameters.Add(param3);
                            scomdst.Parameters.Add(param3_3);
                            SqlParameter param4 = new SqlParameter();
                            param4.ParameterName = "@MOBILENO1";
                            if (hp_no.Length > 0)
                                param4.Value = hp_no[0];
                            else param4.Value = " ";
                            scomdst.Parameters.Add(param4);
                            SqlParameter param4_2 = new SqlParameter();
                            param4_2.ParameterName = "@MOBILENO2";
                            if (hp_no.Length > 1)
                                param4_2.Value = hp_no[1];
                            else
                                param4_2.Value = " ";
                            scomdst.Parameters.Add(param4_2);
                            SqlParameter param4_3 = new SqlParameter();
                            param4_3.ParameterName = "@MOBILENO3";
                            if (hp_no.Length > 2)
                                param4_3.Value = hp_no[2];
                            else
                                param4_3.Value = " ";
                            scomdst.Parameters.Add(param4_3);
                            SqlParameter param5 = new SqlParameter();
                            param5.ParameterName = "@ADDR_GUBUN";
                            param5.Value = "TCM0700001";
                            scomdst.Parameters.Add(param5);
                            SqlParameter param6 = new SqlParameter();
                            param6.ParameterName = "@APPL_POST";
                            string str1 = sdrorg2["zip"].ToString();
                            string str2 = str1.Replace("-", "");
                            param6.Value = str2;
                            scomdst.Parameters.Add(param6);
                            SqlParameter param7 = new SqlParameter();
                            param7.ParameterName = "@APPL_ADDR1";
                            param7.Value = sdrorg2["addr"];
                            scomdst.Parameters.Add(param7);
                            SqlParameter param8 = new SqlParameter();
                            param8.ParameterName = "@APPL_REMARK";
                            param8.Value = sdrorg2["related"];
                            scomdst.Parameters.Add(param8);

                            // 계약자가  이미 들어 있으면 가져옴 
                            g_appl_seq = Int32.Parse(get_APP_ID(param1.Value.ToString(), param3.Value.ToString(), param3_2.Value.ToString(), param3_3.Value.ToString(), param6.Value.ToString()));
                            //int rows = scomdst.ExecuteNonQuery();
                            //계약자 ID 조회  -bury 
                            if (g_appl_seq == -1)
                            {
                                scomdst.CommandText = @"SELECT MAX([APPL_ID])  FROM [funeralsystem_dangjin].[dbo].[applicant]";

                                g_appl_seq = (Int32)scomdst.ExecuteScalar();
                            }
                        }
                       
                        sdrorg2.Close();

                        // seq_moyji로  고인정보에서 고인정보 추출하여 thedaed와 burydead에 저장 
                        scomorg2.CommandText = @"SELECT TOP (1000) [seq]
                                              ,[seq_myoji]
                                              ,[unameb]
                                              ,[mdate]
                                              ,[juminb]
                                              ,[sleepdate]
                                              ,[addrb]
                                              ,[sleepwhy]
                                              ,[sleepon]
                                              ,[age]
                                              ,[btype]
                                              ,[related2]
                                              ,[istop]
                                          FROM [dangjin].[dbo].[tb_bong]
                                          where seq_myoji=@seq_myoji2";

                        SqlParameter param9 = new SqlParameter();
                        param9.ParameterName = "@seq_myoji2";
                        param9.Value = sdrorg[0];
                        scomorg2.Parameters.Add(param9);
                        sdrorg2 = scomorg2.ExecuteReader();

                        int sunbun = 1;//고인순번
                        g_dead_seq = 0;
                        //고인정보가 존재하면 
                        while (sdrorg2.Read())
                        {
                            
                            // 고인정보에 이전 thedead
                            scomdst.CommandText = @"INSERT INTO [dbo].[thedead]
                            ([DEAD_NAME]
                            ,[DEAD_JUMIN]
                            ,[DEAD_AGE]
                            ,[DEAD_DATE]
                            ,[DEAD_PLACE_TXT]
                            ,[DEAD_REASON_TXT]
                            ,[OBJT]
                            ,[ADDR_GUBUN]
                             ,[DEAD_ADDR1]
                             ,[DEAD_ADDR2]
                            )
                        VALUES
                            (@DEAD_NAME
                            ,@DEAD_JUMIN
                            ,@DEAD_AGE
                            ,@DEAD_DATE
                            ,@DEAD_PLACE_TXT
                            ,@DEAD_REASON_TXT
                            ,@OBJT
                            ,@ADDR_GUBUN2
                            ,@DEAD_ADDR1
                            ,'    '
                            )";
                            scomdst.Parameters.Clear();
                            SqlParameter param1 = new SqlParameter();
                            param1.ParameterName = "@DEAD_NAME";
                            param1.Value = sdrorg2["unameb"].ToString();
                            scomdst.Parameters.Add(param1);
                            SqlParameter param2 = new SqlParameter();
                            param2.ParameterName = "@DEAD_JUMIN";
                            param2.Value = sdrorg2["juminb"];
                            scomdst.Parameters.Add(param2);



                            SqlParameter param4 = new SqlParameter();
                            param4.ParameterName = "@DEAD_AGE";
                            param4.Value = sdrorg2["age"];
                            scomdst.Parameters.Add(param4);

                            SqlParameter param5 = new SqlParameter();
                            param5.ParameterName = "@DEAD_DATE";
                            try
                            {
                                var dtStr = "1753-01-01 13:26";
                                param5.Value = DateTime.Parse(sdrorg2["sleepdate"].ToString());
                                if (DateTime.Parse(sdrorg2["sleepdate"].ToString()) <= DateTime.ParseExact(dtStr, "yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture))
                                    param5.Value = DBNull.Value;
                            }
                            catch (Exception ex)
                            {
                                param5.Value = DBNull.Value;
                            }
                            scomdst.Parameters.Add(param5);

                            SqlParameter param6 = new SqlParameter();
                            param6.ParameterName = "@DEAD_PLACE_TXT";
                            param6.Value = sdrorg2["sleepon"].ToString();
                            scomdst.Parameters.Add(param6);

                            SqlParameter param8 = new SqlParameter();
                            param8.ParameterName = "@DEAD_REASON_TXT";
                            param8.Value = sdrorg2["sleepwhy"].ToString();
                            scomdst.Parameters.Add(param8);

                            SqlParameter param8_2 = new SqlParameter();
                            param8_2.ParameterName = "@OBJT";
                            switch (sdrorg["mtype"])
                            {
                                case 0:
                                    param8_2.Value = "TFM0100002";
                                    break;
                                case 1:
                                    param8_2.Value = "TFM0100001";
                                    break;
                                case 2:
                                    param8_2.Value = "TFM0100001";
                                    break;
                                default:
                                    param8_2.Value = "TFM0100002";
                                    break;
                            }
                            switch (mptype)
                            {
                                case "0"://분묘단장
                                    param8_2.Value = "TFM0100001";
                                    break;
                                case "1"://분묘합장
                                    param8_2.Value = "TFM0100001";
                                    break;
                                default:
                                    param8_2.Value = "TFM0100002";
                                    break;
                            }
                            scomdst.Parameters.Add(param8_2);

                            SqlParameter param8_3 = new SqlParameter();
                            param8_3.ParameterName = "@ADDR_GUBUN2";
                            param8_3.Value = "TCM0700001";
                            scomdst.Parameters.Add(param8_3);

                            SqlParameter param8_4 = new SqlParameter();
                            param8_4.ParameterName = "@DEAD_POST";
                            param8_4.Value = " ";
                            scomdst.Parameters.Add(param8_4);

                            SqlParameter param8_5 = new SqlParameter();
                            param8_5.ParameterName = "@DEAD_ADDR1";
                            param8_5.Value = sdrorg2["addrb"].ToString();
                            scomdst.Parameters.Add(param8_5);
                            int rows;
                            String ret = get_DEAD_ID(param1.Value.ToString(), param4.Value.ToString(), param5.Value.ToString());
                            g_dead_seq = Int32.Parse(ret);
                            if (g_dead_seq == -1)
                            {
                                try
                                {
                                    rows = scomdst.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    //Console.WriteLine("check {0}", ex.Message);
                                    ResultLog.LogWrite("1 :" + ex.Message);
                                    String query;
                                    query = scomdst.CommandText;
                                    foreach (SqlParameter parm in scomdst.Parameters)
                                    {
                                        // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                        query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                    }
                                    ResultLog.LogWrite(query);

                                }
                                //사망자  ID 조회 - bury
                                scomdst.CommandText = @"SELECT MAX([DEAD_ID])  FROM [funeralsystem_dangjin].[dbo].[thedead]";

                                g_dead_seq = (Int32)scomdst.ExecuteScalar();
                            }

                            //-- 매장계약별고인정보  insert--
                            scomdst.CommandText = @"INSERT INTO [dbo].[burydead]
                                               ([BURY_SEQ]
                                               ,[DEAD_SEQ]
                                               ,[REAL_DATE]
                                               ,[DEAD_ID]
                                               ,[ISTOP]
                                               ,[DEAD_RELATION_NM]
                                               ,[USE_STATUS])
                                         VALUES
                                               (@BURY_SEQ
                                               ,@DEAD_SEQ
                                               ,@REAL_DATE
                                               ,@DEAD_ID
                                               ,@ISTOP
                                               ,@DEAD_RELATION_NM
                                               ,@USE_STATUS)";
                            SqlParameter pparam1 = new SqlParameter();
                            pparam1.ParameterName = "@BURY_SEQ";
                            pparam1.Value = g_bury_seq;
                            scomdst.Parameters.Add(pparam1);

                            SqlParameter pparam2 = new SqlParameter();
                            pparam2.ParameterName = "@DEAD_SEQ";
                            pparam2.Value = sunbun;
                            scomdst.Parameters.Add(pparam2);

                            SqlParameter pparam3 = new SqlParameter();
                            pparam3.ParameterName = "@REAL_DATE";
                            try
                            {
                                pparam3.Value = DateTime.Parse(sdrorg2["mdate"].ToString());
                            }
                            catch (Exception ex)
                            {
                                pparam3.Value = "";
                            }
                            scomdst.Parameters.Add(pparam3);

                            SqlParameter pparam4 = new SqlParameter();
                            pparam4.ParameterName = "@DEAD_ID";
                            pparam4.Value = g_dead_seq;
                            scomdst.Parameters.Add(pparam4);

                            SqlParameter pparam5 = new SqlParameter();
                            pparam5.ParameterName = "@ISTOP";
                            pparam5.Value = sdrorg2["istop"];
                            scomdst.Parameters.Add(pparam5);


                            SqlParameter pparam7 = new SqlParameter();
                            pparam7.ParameterName = "@DEAD_RELATION_NM";
                            pparam7.Value = sdrorg2["related2"];
                            scomdst.Parameters.Add(pparam7);

                            SqlParameter pparam8 = new SqlParameter();
                            pparam8.ParameterName = "@USE_STATUS";
                            pparam8.Value = 1;
                            scomdst.Parameters.Add(pparam8);

                            rows = scomdst.ExecuteNonQuery();
                            sunbun++;
                            //g_dead_seq++;
                        }
                        sdrorg2.Close();
                        // seq_moyji로  계약정보에서 납부내역을 추출하여 bury와 buryext에 금액및 시작,종료일을 추가한다 

                        // tb_term에서 해당묘지 납부건 조회 
                        {
                            scomorg2.CommandText = @"SELECT [seq]
                                                        ,[seq_myoji]
                                                        ,[ptype]
                                                        ,[pyear]
                                                        ,[psdate]
                                                        ,[pedate]
                                                        ,[pdate]
                                                        ,[pycost]
                                                        ,[pcost]
                                                        ,[npcost]
                                                        ,[petc]
                                                    FROM [dangjin].[dbo].[tb_term]
                                                    where [seq_myoji] = @seq_myoji 
                                                    ";

                            SqlParameter termparam9 = new SqlParameter();
                            termparam9.ParameterName = "@seq_myoji";
                            termparam9.Value = sdrorg[0];
                            scomorg2.Parameters.Add(termparam9);
                            sdrorg2 = scomorg2.ExecuteReader();
                        }
                        int term_total = 0;
                        ext_cnt = 0;
                        //납부내역이 존재하면  필요한 정보추출 
                        DateTime minps, maxpe, maxps;
                        int pcost = 0, t_mgnt_amt = 0, t_use_amt = 0;
                        //가장작은 시작일자와 가장큰 종료 일자로 총기간 산정 
                        minps = DateTime.Parse("2999-01-01");
                        maxpe = DateTime.Parse("1900-01-01");
                        maxps = DateTime.Parse("1900-01-01");
                        while (sdrorg2.Read())
                        {
                            try
                            {
                                if (minps > DateTime.Parse(sdrorg2["psdate"].ToString())) minps = DateTime.Parse(sdrorg2["psdate"].ToString());
                                if (maxpe < DateTime.Parse(sdrorg2["pedate"].ToString())) maxpe = DateTime.Parse(sdrorg2["pedate"].ToString());
                                if (maxps < DateTime.Parse(sdrorg2["pedate"].ToString())) maxps = DateTime.Parse(sdrorg2["pedate"].ToString());
                            }
                            catch (Exception ex)
                            {
                                if (DateTime.Parse("1800-01-01") != minps)
                                    minps = minps;
                                else
                                    minps = DateTime.Parse("1800-01-01");
                                if (DateTime.Parse("1800-01-01") != maxpe)
                                    maxpe = maxpe;
                                else
                                    maxpe = DateTime.Parse("1800-01-01");
                            }
                            pcost += Int32.Parse(sdrorg2["pcost"].ToString());
                            try
                            {
                                switch (Int32.Parse(sdrorg2["ptype"].ToString()))
                                    {
                                        case 0: t_mgnt_amt += Int32.Parse(sdrorg2["pcost"].ToString());
                                                break;
                                        case 1: t_use_amt += Int32.Parse(sdrorg2["pcost"].ToString());
                                                break;

                                }
                            }
                            catch(Exception ex)
                            {
                                int err = 1;
                            }
                            try
                            {
                                receipt_date = DateTime.Parse(sdrorg2["pdate"].ToString());
                            }
                            catch (Exception ex)
                            {
                                receipt_date = maxps;
                            }
                            // term_total +=DateTime.Parse(sdrorg2["pedate"].ToString()).Year - DateTime.Parse(sdrorg2["psdate"].ToString()).Year;
                            //ext_cnt++;
                        }
                        sdrorg2.Close();
                        ext_str_date = minps;
                        ext_end_date = maxpe;
                        term_total = maxpe.Year - minps.Year;
                        // 연장정보가 없는 경우를 제외하고 처리 
                        //기간이 15년 이상  
                        if (term_total >= span)
                            span2 = term_total;
                        else
                            span2 = span;
                        if (span2 == 30) //총기간이 30년 
                        {

                            //2016년 이전에 30년은 무조건 연장 정보 
                            if ((bury_date.Year < 2016) && (span >= 30))
                            {
                                //  연장계약 15년 
                                {
                                    scomdst.Parameters.Clear();
                                    scomdst.CommandText = @"INSERT INTO [dbo].[buryext]
                                                               ([BURY_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                               )
                                                         VALUES
                                                               (@BURY_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@BURY_SEQ";
                                    pparam1.Value = g_bury_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 1;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = minps;  //최초 계약일+15년= 연장시작일
                                    ext_date.AddYears(15);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(15); //연장시작일 +15년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = minps;
                                    scomdst.Parameters.Add(pparam12);

                                    int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }

                                }
                            }
                            //2016년 이후 30년
                            if ((bury_date.Year >= 2016) && (ext_cnt > 1))
                            {
                                //연장계약 15년
                                {
                                    scomdst.Parameters.Clear();
                                    scomdst.CommandText = @"INSERT INTO [dbo].[buryext]
                                                               ([BURY_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                               )
                                                         VALUES
                                                               (@BURY_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@BURY_SEQ";
                                    pparam1.Value = g_bury_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 1;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = minps;  //최초 계약일+15년= 연장시작일
                                    ext_date.AddYears(15);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(15); //연장시작일 +15년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = minps;
                                    scomdst.Parameters.Add(pparam12);
                                    int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }

                                }
                            }
                        }
                        if (span2 == 45)
                        {

                            if (ext_cnt == 3)
                            {
                                // 연장정보가 3건이면 15+15+15} 추가는 15,15
                                //연장계약 15년
                                {
                                    scomdst.Parameters.Clear();
                                    scomdst.CommandText = @"INSERT INTO [dbo].[buryext]
                                                               ([BURY_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                               )
                                                         VALUES
                                                               (@BURY_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                               
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@BURY_SEQ";
                                    pparam1.Value = g_bury_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 1;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = minps;  //최초 계약일+15년= 연장시작일
                                    ext_date.AddYears(15);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(15); //연장시작일 +15년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = pcost / 3;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = pcost / 3;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = pcost / 3;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = minps;
                                    scomdst.Parameters.Add(pparam12);
                                    int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }

                                }
                                //연장계약 15년 두번째
                                {
                                    scomdst.Parameters.Clear();
                                    scomdst.CommandText = @"INSERT INTO [dbo].[buryext]
                                                               ([BURY_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE])
                                                         VALUES
                                                               (@BURY_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@BURY_SEQ";
                                    pparam1.Value = g_bury_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 2;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = minps;  //최초 계약일+30년= 연장시작일
                                    ext_date.AddYears(30);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(15); //연장시작일 +15년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = pcost / 3;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = pcost / 3;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = pcost / 3;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = minps.AddYears(15);
                                    scomdst.Parameters.Add(pparam12);
                                    int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }

                                }

                            }
                            scomdst.Parameters.Clear();
                            if (ext_cnt == 2)
                            {
                                // 연장정보가 2건이면 15+30} 추가는 30 
                                //연장계약 30년
                                {
                                    scomdst.CommandText = @"INSERT INTO [dbo].[buryext]
                                                               ([BURY_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                                 )
                                                         VALUES
                                                               (@BURY_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE 
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@BURY_SEQ";
                                    pparam1.Value = g_bury_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 1;
                                    g_ext_no = 1;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = minps;  //최초 계약일+15년= 연장시작일
                                    ext_date.AddYears(15);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(30); //연장시작일 +30년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = pcost / 2;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = minps;
                                    scomdst.Parameters.Add(pparam12);
                                    int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }

                                }
                            }
                        }
                        try
                        {
                            str_date = DateTime.Parse(sdrorg["ydate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            str_date = DateTime.Now;
                        }
                        //bury insert  
                        {
                            scomdst.Parameters.Clear();
                            scomdst.CommandText = @"INSERT INTO [dbo].[bury]
                                                               ([FAC_ID]
                                                               ,[BURY_TYPE]
                                                               ,[BURY_DATE]
                                                               ,[SET_DATE]
                                                               ,[BURY_KIND]
                                                               ,[LOC_CODE]
                                                               ,[DAN_NO]
                                                               ,[ROW_NO]
                                                               ,[COL_NO]
                                                               ,[BURY_NO]
                                                               ,[BURY_NO2]
                                                               ,[APPL_ID]
                                                               ,[STR_DATE]
                                                               ,[END_DATE]
                                                               ,[EXT_CNT]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[USE_GUBUN]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[GRASS_AMT]
                                                               ,[RECEIPT_DATE]
                                                               ,[MADECOMP]
                                                               ,[MNO]
                                                               ,[REMARK])
                                                         VALUES
                                                               (@FAC_ID,
                                                               @BURY_TYPE,
                                                               @BURY_DATE, 
                                                               @SET_DATE, 
                                                               @BURY_KIND, 
                                                               @LOC_CODE, 
                                                               @DAN_NO, 
                                                               @ROW_NO, 
                                                               @COL_NO, 
                                                               @BURY_NO, 
                                                               @BURY_NO2,
                                                               @APPL_ID, 
                                                               @STR_DATE, 
                                                               @END_DATE, 
                                                               @EXT_CNT, 
                                                               @EXT_STR_DATE, 
                                                               @EXT_END_DATE, 
                                                               @USE_GUBUN, 
                                                               @DC_GUBUN, 
                                                               @USE_ORI_AMT, 
                                                               @USE_DC_AMT, 
                                                               @USE_AMT, 
                                                               @MGMT_AMT, 
                                                               @GRASS_AMT, 
                                                               @RECEIPT_DATE, 
                                                               @MADECOMP, 
                                                               @MNO,
                                                                @REMARK)";
                            SqlParameter pparam1 = new SqlParameter();
                            pparam1.ParameterName = "@FAC_ID";
                            pparam1.Value = fac_id;
                            scomdst.Parameters.Add(pparam1);

                            SqlParameter pparam2 = new SqlParameter();
                            pparam2.ParameterName = "@BURY_TYPE";
                            pparam2.Value = bury_type;
                            scomdst.Parameters.Add(pparam2);

                            SqlParameter pparam3 = new SqlParameter();
                            pparam3.ParameterName = "@BURY_DATE";
                            pparam3.Value = bury_date;
                            scomdst.Parameters.Add(pparam3);

                            SqlParameter pparam5 = new SqlParameter();
                            pparam5.ParameterName = "@SET_DATE";
                            pparam5.Value = set_date;
                            scomdst.Parameters.Add(pparam5);


                            SqlParameter pparam7 = new SqlParameter();
                            pparam7.ParameterName = "@BURY_KIND";
                            pparam7.Value = bury_kind;
                            scomdst.Parameters.Add(pparam7);

                            SqlParameter pparam8 = new SqlParameter();
                            pparam8.ParameterName = "@ROW_NO";
                            pparam8.Value =0;
                            scomdst.Parameters.Add(pparam8);

                            SqlParameter pparam9 = new SqlParameter();
                            pparam9.ParameterName = "@COL_NO";
                            pparam9.Value = 0;
                            scomdst.Parameters.Add(pparam9);

                            SqlParameter pparam10 = new SqlParameter();
                            pparam10.ParameterName = "@BURY_NO2";
                            pparam10.Value = bury_no2;
                            scomdst.Parameters.Add(pparam10);

                            SqlParameter pparam11 = new SqlParameter();
                            pparam11.ParameterName = "@APPL_ID";
                            pparam11.Value = g_appl_seq;
                            scomdst.Parameters.Add(pparam11);

                            SqlParameter pparam12 = new SqlParameter();
                            pparam12.ParameterName = "@STR_DATE";
                            pparam12.Value = str_date;
                            scomdst.Parameters.Add(pparam12);

                            SqlParameter pparam12_2 = new SqlParameter();
                            pparam12_2.ParameterName = "@END_DATE";
                            pparam12_2.Value = end_date;
                            scomdst.Parameters.Add(pparam12_2);

                            SqlParameter pparam13 = new SqlParameter();
                            pparam13.ParameterName = "@EXT_STR_DATE";
                            if (ext_str_date != DateTime.Parse("2999-01-01"))
                                pparam13.Value = ext_str_date;
                            else
                                pparam13.Value = DBNull.Value;
                            scomdst.Parameters.Add(pparam13);

                            SqlParameter pparam14 = new SqlParameter();
                            pparam14.ParameterName = "@EXT_END_DATE";
                            if (ext_end_date != DateTime.Parse("1900-01-01"))
                                pparam14.Value = ext_end_date;
                            else
                                pparam14.Value = DBNull.Value;
                            scomdst.Parameters.Add(pparam14);

                            SqlParameter pparam15 = new SqlParameter();
                            pparam15.ParameterName = "@EXT_CNT";
                            ext_cnt = 0;
                            if (ext_cnt >= 2) ext_cnt = g_ext_no;
                            //연장횟수 무조건 0으로 
                            ext_cnt = 0;
                            pparam15.Value = ext_cnt;
                            scomdst.Parameters.Add(pparam15);

                            SqlParameter pparam16 = new SqlParameter();
                            pparam16.ParameterName = "@USE_GUBUN";
                            use_gubun = "U";
                            switch (sdrorg["gtype"])
                            {
                                case 0: use_gubun = "U";
                                    break;
                                case 2:use_gubun = "B";
                                        break;
                                case 4:use_gubun = "X";
                                    break;
                            }
                            pparam16.Value = use_gubun;
                            scomdst.Parameters.Add(pparam16);

                            SqlParameter pparam17 = new SqlParameter();
                            pparam17.ParameterName = "@DC_GUBUN";
                            pparam17.Value = "TCM1200001";
                            scomdst.Parameters.Add(pparam17);

                            SqlParameter pparam18 = new SqlParameter();
                            pparam18.ParameterName = "@USE_ORI_AMT";
                            pparam18.Value = sdrorg["usecost"];
                            scomdst.Parameters.Add(pparam18);

                            SqlParameter pparam19 = new SqlParameter();
                            pparam19.ParameterName = "@USE_DC_AMT";
                            pparam19.Value = 0;
                            scomdst.Parameters.Add(pparam19);

                            SqlParameter pparam20 = new SqlParameter();
                            pparam20.ParameterName = "@USE_AMT";
                            if (Int32.Parse(sdrorg["usecost"].ToString()) != 0)
                                pparam20.Value = sdrorg["usecost"];
                            else
                                pparam20.Value = pcost;
                            scomdst.Parameters.Add(pparam20);

                            SqlParameter pparam21 = new SqlParameter();
                            pparam21.ParameterName = "@MGMT_AMT";
                            pparam21.Value = 0;
                            scomdst.Parameters.Add(pparam21);


                            SqlParameter pparam22 = new SqlParameter();
                            pparam22.ParameterName = "@GRASS_AMT";
                            pparam22.Value = 0;
                            scomdst.Parameters.Add(pparam22);

                            SqlParameter pparam23 = new SqlParameter();
                            pparam23.ParameterName = "@RECEIPT_DATE";
                            pparam23.Value = receipt_date;
                            scomdst.Parameters.Add(pparam23);

                            SqlParameter pparam24 = new SqlParameter();
                            pparam24.ParameterName = "@MADECOMP";
                            pparam24.Value = sdrorg["madecomp"];
                            scomdst.Parameters.Add(pparam24);


                            SqlParameter pparam25 = new SqlParameter();
                            pparam25.ParameterName = "@MNO";
                            pparam25.Value = sdrorg["mno"];
                            scomdst.Parameters.Add(pparam25);

                            SqlParameter pparam26 = new SqlParameter();
                            pparam26.ParameterName = "@LOC_CODE";
                            pparam26.Value = loc_code;
                            scomdst.Parameters.Add(pparam26);

                            SqlParameter pparam27= new SqlParameter();
                            pparam27.ParameterName = "@DAN_NO";
                            pparam27.Value = dan_no;
                            scomdst.Parameters.Add(pparam27);

                            SqlParameter pparam28 = new SqlParameter();
                            pparam28.ParameterName = "@BURY_NO";
                            pparam28.Value = bury_no;
                            scomdst.Parameters.Add(pparam28);

                            SqlParameter pparam29 = new SqlParameter();
                            pparam29.ParameterName = "@REMARK";
                            if (sdrorg["etc"].ToString().Length > 200)
                                pparam29.Value = sdrorg["etc"].ToString().Substring(0, 200);
                            else
                                pparam29.Value = sdrorg["etc"];
                            scomdst.Parameters.Add(pparam29);
                            int rows = scomdst.ExecuteNonQuery();
                            {
                                String query;
                                query = scomdst.CommandText;
                                foreach (SqlParameter parm in scomdst.Parameters)
                                {
                                    // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                    query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                }

                                ResultLog.LogWrite(query);
                            }

                        }
                        //sconorg.Open();
                        {

                        }


                        g_bury_seq++;
                        break;

                    case "3": 
                    case "4":
                    case "10":
                       
                        //3:납골유연, 4:납골무연, 10.납골부부 는 enshine( 봉안계약) 
                        bury_type = "TFM0400001";
                        bury_kind = "TFM0500001";
  
                         switch(mno_code[5])
                        {
                            case "1":
                                bury_type = "TFM0400001";
                                bury_kind = "TFM0500001";
                                break;
                            case "2":
                                bury_type = "TFM0400004";
                                bury_kind = "TFM0500002";
                                break;
                            case "3":
                                bury_type = "TFM1000001";
                                bury_kind = "TFM0500001";
                                break;
                            case "4":
                                bury_type = "TFM1000004";
                                bury_kind = "TFM0500001";
                                break;
                            case "5":
                                bury_type = "TFM0400001";
                                bury_kind = "TFM0500001";
                                break;
                            case "6":
                                bury_type = "TFM0400001";
                                bury_kind = "TFM0500001";
                                break;
                            case "7":
                                bury_type = "TFM0400004";
                                bury_kind = "TFM0500002";
                                break;
                            case "8":
                                bury_type = "TFM0400001";
                                bury_kind = "TFM0500001";
                                break;
                            case "9":
                                bury_type = "TFM0400003";
                                bury_kind = "TFM0500001";
                                break;
                            case "10":
                                bury_type = "TFM1000003";
                                bury_kind = "TFM0500002";
                                break;
                        }

                        switch (mno_code[1]) //구역번호 처리 
                        {
                            case "가":
                                loc_code = "01";
                                break;
                            case "나":
                                loc_code = "02";
                                break;
                            default:
                                loc_code = mno_code[1];
                                break;

                        }

                        try
                        {
                            bury_no = Int32.Parse(mno_code[3]);
                        }
                        catch (Exception ex)
                        {
                            bury_no = 0;
                        }
                        try
                        { 
                            bury_no2 = Int32.Parse(mno_code[4]);
                        }
                        catch (Exception ex)
                        {
                            bury_no2 = 0;
                        }

                        fac_id = mno_code[0];
                        //bury_type = "TFM0400001";
                        //bury_kind = "TFM0500001";
                        receipt_date = DateTime.Now;
                        //행,열 번호 초기화 
                        row_no = 0;
                        col_no = 0;
                        span = 0;
                        //안치형태 봉안
                        String enstype = "";
                        //enstype = sdrorg["mtype"].ToString();
                        enstype = mno_code[5];
                        switch (enstype)
                        {
                            case "0"://분묘단장
                                bury_type = "TFM1000001";//개인단
                                  break;
                            case "1"://분묘합장
                                bury_type = "TFM1000001";//개인단
                                break;
                            case "2"://납골개인
                                bury_type = "TFM1000001";//개인단
                                break;
                            case "3"://납골유연
                                bury_type = "TFM1000001";//개인단
                                break;
                            case "4"://납골무연
                                bury_type = "TFM1000004";//개인단
                                break;
                            case "5"://개인봉안묘합장
                                bury_type = "TFM1000005";//가족단
                                break;
                            case "6"://가족납골묘
                                bury_type = "TFM1000005";//가족단
                                  break;
                            case "7": //자연장
                                bury_type = "TFM0400001";//분묘
                                break;
                            case "8"://납골부부
                                bury_type = "TFM1000003";//부부단
                                break;
                            case "9"://행려분묘=>봉안평장
                                bury_type = "TFM1000004";//행려분묘=>봉안평장
                                break;
                            case "10": //납골부부
                                bury_type = "TFM1000003";//부부단
                                break;
                        }
                        //debug
                        //if (mno_code[3].Equals("0583"))
                        //{
                        //    bury_type = bury_type;
                        //}
                        //계약일자
                        try
                        {
                            bury_date = DateTime.Parse(sdrorg["ydate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            bury_date = DateTime.Now;
                        }
                        //설치일자 
                        try
                        {
                            set_date = DateTime.Parse(sdrorg["setdate"].ToString());
                        }
                        catch(Exception ex)
                        {
                            set_date = DateTime.Now;
                        }
                        //만기일자 
                        try
                        {
                            end_date = DateTime.Parse(sdrorg["expdate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            end_date = DateTime.Now;
                        }
                        //계약기간 계산 
                        span = end_date.Year - bury_date.Year;
                        ext_cnt = 0;
                        scomorg2.Parameters.Clear();
                        try
                        {
                            bury_date = DateTime.Parse(sdrorg["ydate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            bury_date = DateTime.Now;
                        }
                        //설치일자 
                        try
                        {
                            set_date = DateTime.Parse(sdrorg["setdate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            set_date = DateTime.Now;
                        }
                        //만기일자 
                        try
                        {
                            end_date = DateTime.Parse(sdrorg["expdate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            end_date = DateTime.Now;
                        }
                        //계약기간 계산 
                        span = end_date.Year - bury_date.Year;
                        ext_cnt = 0;
                        scomorg2.Parameters.Clear();
                        // tb_cust 에서 계약자 정보 꺼냄
                        scomorg2.CommandText = @"SELECT TOP (1000) [seq]
                                                  ,[unamea]
                                                  ,[jumin]
                                                  ,[tel]
                                                  ,[hp]
                                                  ,[email]
                                                  ,[zip]
                                                  ,[addr]
                                                  ,[related]
                                              FROM [dangjin].[dbo].[tb_cust]
                                              where [seq]=@seq_cust";
                        SqlParameter ens_param = new SqlParameter();
                        ens_param.ParameterName = "@seq_cust";
                        ens_param.Value = sdrorg["seq_customer"];
                        scomorg2.Parameters.Add(ens_param);
                        sdrorg2 = scomorg2.ExecuteReader();
                        g_appl_seq = 0;
                        //계약자 정보 이전  
                        while (sdrorg2.Read())
                        {
                            //g_appl_seq++;
                            scomdst.CommandText = @"INSERT INTO [dbo].[applicant]
                                                   ([APPL_NAME]
                                                   ,[APPL_JUMIN]
                                                   ,[TELNO1]
                                                   ,[TELNO2]
                                                   ,[TELNO3]
                                                   ,[MOBILENO1]
                                                   ,[MOBILENO2]
                                                   ,[MOBILENO3]
                                                   ,[ADDR_GUBUN]
                                                   ,[APPL_POST]
                                                   ,[APPL_ADDR1]
                                                   ,[APPL_ADDR2]
                                                   ,[APPL_REMARK])
                                             VALUES
                                                   (@APPL_NAME
                                                   ,@APPL_JUMIN
                                                   ,@TELNO1
                                                   ,@TELNO2
                                                   ,@TELNO3
                                                   ,@MOBILENO1
                                                   ,@MOBILENO2
                                                   ,@MOBILENO3
                                                   ,@ADDR_GUBUN
                                                   ,@APPL_POST
                                                   ,@APPL_ADDR1
                                                   ,'   ','   ')";
                            //scomdst.CommandText = @"INSERT INTO [dbo].[applicant]
                            //                       ([APPL_NAME]
                            //                       ,[APPL_JUMIN]
                            //                       ,[TELNO1]
                            //                       ,[TELNO2]
                            //                       ,[TELNO3]
                            //                       ,[MOBILENO1]
                            //                       ,[MOBILENO2]
                            //                       ,[MOBILENO3]
                            //                       ,[ADDR_GUBUN]
                            //                       ,[APPL_POST]
                            //                       ,[APPL_ADDR1]
                            //                       ,[APPL_REMARK])
                            //                 VALUES
                            //                       (@APPL_NAME
                            //                       ,@APPL_JUMIN
                            //                       ,@TELNO1
                            //                       ,@TELNO2
                            //                       ,@TELNO3
                            //                       ,@MOBILENO1
                            //                       ,@MOBILENO2
                            //                       ,@MOBILENO3
                            //                       ,@ADDR_GUBUN
                            //                       ,@APPL_POST
                            //                       ,@APPL_ADDR1
                            //                       ,@APPL_REMARK)";

                            string[] tel_no = sdrorg2["tel"].ToString().Split('-');
                            string[] hp_no = sdrorg2["hp"].ToString().Split('-');

                            //SqlParameter param0 = new SqlParameter();
                            //param0.ParameterName = "@APPL_ID";
                            //param0.Value = g_appl_seq;
                            //scomdst.Parameters.Add(param0);
                            SqlParameter param1 = new SqlParameter();
                            param1.ParameterName = "@APPL_NAME";
                            param1.Value = sdrorg2["unamea"];
                            scomdst.Parameters.Add(param1);
                            SqlParameter param2 = new SqlParameter();
                            param2.ParameterName = "@APPL_JUMIN";
                            param2.Value = sdrorg2["jumin"];
                            scomdst.Parameters.Add(param2);
                            SqlParameter param3 = new SqlParameter();
                            param3.ParameterName = "@TELNO1";
                            if (tel_no.Length < 3)
                                param3.Value = "041";
                            else
                                param3.Value = tel_no[0];

                            SqlParameter param3_2 = new SqlParameter();
                            param3_2.ParameterName = "@TELNO2";
                            if (tel_no.Length < 3)
                                param3_2.Value = tel_no[0];
                            else
                                param3_2.Value = tel_no[1];
                            scomdst.Parameters.Add(param3_2);
                            SqlParameter param3_3 = new SqlParameter();
                            param3_3.ParameterName = "@TELNO3";
                            if (tel_no.Length > 1)
                            {
                                if (tel_no.Length < 3)
                                    param3_3.Value = tel_no[0];
                                else
                                    param3_3.Value = tel_no[2];
                            }
                            else param3_3.Value = "";
                            if (tel_no.Length <= 1)
                            {
                                param3.Value = "";
                                param3_2.Value = "";
                                param3_3.Value = "";

                            }

                            if (param3_2.Value.Equals("")) param3.Value = "";
                            scomdst.Parameters.Add(param3);
                            scomdst.Parameters.Add(param3_3);
                            SqlParameter param4 = new SqlParameter();

                            param4.ParameterName = "@MOBILENO1";
                            if (hp_no.Length > 0)
                                param4.Value = hp_no[0];
                            else param4.Value = " ";
                            scomdst.Parameters.Add(param4);
                            SqlParameter param4_2 = new SqlParameter();
                            param4_2.ParameterName = "@MOBILENO2";
                            if (hp_no.Length > 1)
                                param4_2.Value = hp_no[1];
                            else
                                param4_2.Value = " ";
                            scomdst.Parameters.Add(param4_2);
                            SqlParameter param4_3 = new SqlParameter();
                            param4_3.ParameterName = "@MOBILENO3";
                            if (hp_no.Length > 2)
                                param4_3.Value = hp_no[2];
                            else
                                param4_3.Value = " ";
                            scomdst.Parameters.Add(param4_3);

                            SqlParameter param5 = new SqlParameter();
                            param5.ParameterName = "@ADDR_GUBUN";
                            param5.Value = "TCM0700001";
                            scomdst.Parameters.Add(param5);
 
                            SqlParameter param6 = new SqlParameter();
                            param6.ParameterName = "@APPL_POST";
                            string str1 = sdrorg2["zip"].ToString();
                            string str2 = str1.Replace("-","");
                            param6.Value = str2;
                            scomdst.Parameters.Add(param6);

                            SqlParameter param7 = new SqlParameter();
                            param7.ParameterName = "@APPL_ADDR1";
                            param7.Value = sdrorg2["addr"];
                            scomdst.Parameters.Add(param7);
                            SqlParameter param8 = new SqlParameter();
                            param8.ParameterName = "@APPL_REMARK";
                            param8.Value = sdrorg2["related"];
                            scomdst.Parameters.Add(param8);


                            // 계약자가  이미 들어 있으면 가져옴 
                            g_appl_seq = Int32.Parse(get_APP_ID(param1.Value.ToString(), param3.Value.ToString(), param3_2.Value.ToString(), param3_3.Value.ToString(), param6.Value.ToString()));
                            //int rows = scomdst.ExecuteNonQuery();
                            //계약자 ID 조회 
                            if (g_appl_seq == -1)
                            {
                                scomdst.CommandText = @"SELECT MAX([APPL_ID])  FROM [funeralsystem_dangjin].[dbo].[applicant]";

                                g_appl_seq = (Int32)scomdst.ExecuteScalar();
                            }
                        }
                        sdrorg2.Close();

                        // seq_moyji로  고인정보에서 고인정보 추출하여 thedead와ensdead에 저장 
                        scomorg2.CommandText = @"SELECT TOP (1000) [seq]
                                              ,[seq_myoji]
                                              ,[unameb]
                                              ,[mdate]
                                              ,[juminb]
                                              ,[sleepdate]
                                              ,[addrb]
                                              ,[sleepwhy]
                                              ,[sleepon]
                                              ,[age]
                                              ,[btype]
                                              ,[related2]
                                              ,[istop]
                                          FROM [dangjin].[dbo].[tb_bong]
                                          where seq_myoji=@seq_myoji2";

                        SqlParameter ens_param9 = new SqlParameter();
                        ens_param9.ParameterName = "@seq_myoji2";
                        ens_param9.Value = sdrorg[0];
                        scomorg2.Parameters.Add(ens_param9);
                        sdrorg2 = scomorg2.ExecuteReader();

                        int ens_sunbun = 1;//고인순번
                        g_dead_seq = 0;
                        //고인정보가 존재하면 
                        while (sdrorg2.Read())
                        {

                            // 고인정보에 이전 thedead_damo
                            scomdst.CommandText = @"INSERT INTO [dbo].[thedead]
                            ([DEAD_NAME]
                            ,[DEAD_JUMIN]
                            ,[DEAD_AGE]
                            ,[DEAD_DATE]
                            ,[DEAD_PLACE_TXT]
                            ,[DEAD_REASON_TXT]
                            ,[OBJT]
                            ,[ADDR_GUBUN]
                             ,[DEAD_ADDR1]
                            ,[DEAD_ADDR2]
                            )
                        VALUES
                            (@DEAD_NAME
                            ,@DEAD_JUMIN
                            ,@DEAD_AGE
                            ,@DEAD_DATE
                            ,@DEAD_PLACE_TXT
                            ,@DEAD_REASON_TXT
                            ,@OBJT
                            ,@ADDR_GUBUN2
                            ,@DEAD_ADDR1
                            , '   '
                            )";
                            scomdst.Parameters.Clear();
                            SqlParameter param1 = new SqlParameter();
                            param1.ParameterName = "@DEAD_NAME";
                            param1.Value = sdrorg2["unameb"].ToString();
                            scomdst.Parameters.Add(param1);
                            SqlParameter param2 = new SqlParameter("@DEAD_JUMIN", SqlDbType.NVarChar,14);
                            param2.Value = sdrorg2["juminb"];
                            scomdst.Parameters.Add(param2);



                            SqlParameter param4 = new SqlParameter();
                            param4.ParameterName = "@DEAD_AGE";
                            param4.Value = sdrorg2["age"];
                            scomdst.Parameters.Add(param4);

                            SqlParameter param5 = new SqlParameter();
                            param5.ParameterName = "@DEAD_DATE";
                            try
                            {
                                var dtStr = "1753-01-01 13:26";
                                param5.Value = DateTime.Parse(sdrorg2["sleepdate"].ToString());
                                if (DateTime.Parse(sdrorg2["sleepdate"].ToString()) <= DateTime.ParseExact(dtStr, "yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture))
                                    param5.Value = DBNull.Value;
                            }
                            catch (Exception ex)
                            {
                                param5.Value = DBNull.Value;
                            }
                            scomdst.Parameters.Add(param5);

                            SqlParameter param6 = new SqlParameter();
                            param6.ParameterName = "@DEAD_PLACE_TXT";
                            param6.Value = sdrorg2["sleepon"].ToString();
                            scomdst.Parameters.Add(param6);

                            SqlParameter param8 = new SqlParameter();
                            param8.ParameterName = "@DEAD_REASON_TXT";
                            param8.Value = sdrorg2["sleepwhy"].ToString();
                            scomdst.Parameters.Add(param8);

                            SqlParameter param8_2 = new SqlParameter();
                            param8_2.ParameterName = "@OBJT";
                            mptype = sdrorg["mtype"].ToString();
                            switch (mptype)
                            {
                                case "0"://분묘단장
                                    param8_2.Value = "TFM0100001";
                                    break;
                                case "1"://분묘합장
                                    param8_2.Value = "TFM0100001";
                                    break;
                                default:
                                    param8_2.Value = "TFM0100002";
                                    break;
                            }

                            scomdst.Parameters.Add(param8_2);

                            SqlParameter param8_3 = new SqlParameter();
                            param8_3.ParameterName = "@ADDR_GUBUN2";
                            param8_3.Value = "TCM0700001";
                            scomdst.Parameters.Add(param8_3);

                            SqlParameter param8_4 = new SqlParameter();
                            param8_4.ParameterName = "@DEAD_POST";
                            param8_4.Value = " ";
                            scomdst.Parameters.Add(param8_4);

                            SqlParameter param8_5 = new SqlParameter();
                            param8_5.ParameterName = "@DEAD_ADDR1";
                            param8_5.Value = sdrorg2["addrb"].ToString();
                            scomdst.Parameters.Add(param8_5);
                            g_dead_seq = Int32.Parse(get_DEAD_ID(param1.Value.ToString(), param4.Value.ToString(), param5.Value.ToString()));
                            int rows;
                            if (g_dead_seq == -1)
                            {
                                rows = scomdst.ExecuteNonQuery();
                                //계약자 ID 조회 
                                scomdst.CommandText = @"SELECT MAX([DEAD_ID])  FROM [funeralsystem_dangjin].[dbo].[thedead]";

                                g_dead_seq = (Int32)scomdst.ExecuteScalar();
                            }
                            scomdst.Parameters.Clear();
                            //-- 봉안계약별고인정보 -- 
                            scomdst.CommandText = @"INSERT INTO [dbo].[ensdead]
                                                               ([ENS_SEQ]
                                                               ,[DEAD_SEQ]
                                                               ,[REAL_DATE]
                                                               ,[DEAD_ID]
                                                               ,[ISTOP]
                                                               ,[DEAD_RELATION_NM]
                                                               ,[USE_STATUS])
                                                         VALUES
                                                               (@ENS_SEQ, 
                                                               @DEAD_SEQ,
                                                               @REAL_DATE,
                                                               @DEAD_ID,
                                                               @ISTOP,
                                                               @DEAD_RELATION_NM,
                                                               @USE_STATUS )";
                            SqlParameter pparam1 = new SqlParameter();
                            pparam1.ParameterName = "@ENS_SEQ";
                            pparam1.Value = g_ens_seq;
                            scomdst.Parameters.Add(pparam1);

                            SqlParameter pparam2 = new SqlParameter();
                            pparam2.ParameterName = "@DEAD_SEQ";
                            pparam2.Value = ens_sunbun;
                            scomdst.Parameters.Add(pparam2);

                            SqlParameter pparam3 = new SqlParameter();
                            pparam3.ParameterName = "@REAL_DATE";
                            try
                            {
                                var dtStr = "1753-01-01 13:26";
                                pparam3.Value = DateTime.Parse(sdrorg2["mdate"].ToString());
                                if (DateTime.Parse(sdrorg2["mdate"].ToString()) <= DateTime.ParseExact(dtStr, "yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture))
                                    pparam3.Value = DBNull.Value;
                               
                            }
                            catch (Exception ex)
                            {
                                pparam3.Value = DBNull.Value;
                            }
                            scomdst.Parameters.Add(pparam3);

                            SqlParameter pparam4 = new SqlParameter();
                            pparam4.ParameterName = "@DEAD_ID";
                            pparam4.Value = g_dead_seq;
                            scomdst.Parameters.Add(pparam4);

                            SqlParameter pparam5 = new SqlParameter();
                            pparam5.ParameterName = "@ISTOP";
                            pparam5.Value = sdrorg2["istop"];
                            scomdst.Parameters.Add(pparam5);


                            SqlParameter pparam7 = new SqlParameter();
                            pparam7.ParameterName = "@DEAD_RELATION_NM";
                            pparam7.Value = sdrorg2["related2"];
                            scomdst.Parameters.Add(pparam7);

                            SqlParameter pparam8 = new SqlParameter();
                            pparam8.ParameterName = "@USE_STATUS";
                            pparam8.Value = 1;
                            scomdst.Parameters.Add(pparam8);

                            rows = scomdst.ExecuteNonQuery();

                            ens_sunbun++;
                           // g_dead_seq++;
                        }
                        sdrorg2.Close();
                        ///----------------------------------------------------------------------------------------
                        // seq_moyji로  계약정보에서 납부내역을 추출하여 bury와 buryext에 금액및 시작,종료일을 추가한다 

                        // tb_term에서 해당묘지 납부건 조회 
                        {
                            scomorg2.CommandText = @"SELECT [seq]
                                                        ,[seq_myoji]
                                                        ,[ptype]
                                                        ,[pyear]
                                                        ,[psdate]
                                                        ,[pedate]
                                                        ,[pdate]
                                                        ,[pycost]
                                                        ,[pcost]
                                                        ,[npcost]
                                                        ,[petc]
                                                    FROM [dangjin].[dbo].[tb_term]
                                                    where [seq_myoji] = @seq_myoji
                                                    ";

                            SqlParameter termparam9 = new SqlParameter();
                            termparam9.ParameterName = "@seq_myoji";
                            termparam9.Value = sdrorg[0];
                            scomorg2.Parameters.Add(termparam9);
                            sdrorg2 = scomorg2.ExecuteReader();
                        }
                        int ens_term_total = 0;
                        ext_cnt = 0;
                        //납부내역이 존재하면  필요한 정보추출 
                        DateTime ens_minps, ens_maxpe, ens_maxps;
                        int ens_pcost = 0;
                        t_mgnt_amt = 0;
                        t_use_amt = 0;
                        //가장작은 시작일자와 가장큰 종료 일자로 총기간 산정 
                        ens_minps = DateTime.Parse("2999-01-01");
                        ens_maxpe = DateTime.Parse("1900-01-01");
                        ens_maxps = DateTime.Parse("1900-01-01");
                        while (sdrorg2.Read())
                        {
                            try
                            {
                                if (ens_minps > DateTime.Parse(sdrorg2["psdate"].ToString())) ens_minps = DateTime.Parse(sdrorg2["psdate"].ToString());
                                if (ens_maxpe < DateTime.Parse(sdrorg2["pedate"].ToString())) ens_maxpe = DateTime.Parse(sdrorg2["pedate"].ToString());
                                if (ens_maxps < DateTime.Parse(sdrorg2["pedate"].ToString())) ens_maxps = DateTime.Parse(sdrorg2["pedate"].ToString());
                            }
                            catch (Exception ex)
                            {  if (DateTime.Parse("1800-01-01") != ens_minps)
                                    ens_minps = ens_minps;
                                else
                                    ens_minps = DateTime.Parse("1800-01-01");
                                if (DateTime.Parse("1800-01-01") != ens_maxpe)
                                    ens_maxpe = ens_maxpe;
                                else
                                    ens_maxpe = DateTime.Parse("1800-01-01");
                            }
                            ens_pcost += Int32.Parse(sdrorg2["pcost"].ToString());
                            try
                            {
                                switch (Int32.Parse(sdrorg2["ptype"].ToString()))
                                {
                                    case 0:
                                        t_mgnt_amt += Int32.Parse(sdrorg2["pcost"].ToString());
                                        break;
                                    case 1:
                                        t_use_amt += Int32.Parse(sdrorg2["pcost"].ToString());
                                        break;

                                }
                            }
                              catch (Exception ex)
                            {
                                int err = 1;
                            }
                            receipt_date = DateTime.Now;
                            try
                            {
                                receipt_date = DateTime.Parse(sdrorg2["pdate"].ToString());
                            }
                            catch (Exception ex)
                            {
                                receipt_date = ens_maxps;
                            }
                            // term_total +=DateTime.Parse(sdrorg2["pedate"].ToString()).Year - DateTime.Parse(sdrorg2["psdate"].ToString()).Year;
                            ext_cnt++;
                        }
                        sdrorg2.Close();
                        ext_str_date = ens_minps;
                        ext_end_date =ens_maxpe;
                        ens_term_total = ens_maxpe.Year - ens_minps.Year;
                        // 연장정보가 없는 경우를 제외하고 처리 
                        //기간이 15년 이상  
                        if (ens_term_total >= span)
                            span2 = ens_term_total;
                        else
                            span2 = span;
                        if (span2 == 30) //총기간이 30년 
                        {

                            //2016년 이전에 30년은 무조건 연장 정보 
                            if ((bury_date.Year < 2016) && (span >= 30))
                            {
                                //  연장계약 15년 
                                {
                                    scomdst.Parameters.Clear();
                                    scomdst.CommandText = @"INSERT INTO [dbo].[ensext]
                                                               ([ENS_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                               )
                                                         VALUES
                                                               (@ENS_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@ENS_SEQ";
                                    pparam1.Value = g_ens_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 1;
                                    g_ext_no = 1;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = ens_minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = ens_minps;  //최초 계약일+15년= 연장시작일
                                    ext_date.AddYears(15);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(15); //연장시작일 +15년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = ens_minps;
                                    scomdst.Parameters.Add(pparam12);
                                    int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }

                                }
                            }
                             //2016년 이후 30년
                            else if ((bury_date.Year >= 2016) && (ext_cnt > 1))
                            {
                                //연장계약 15년
                                {
                                    scomdst.Parameters.Clear();
                                    scomdst.CommandText = @"INSERT INTO [dbo].[ensext]
                                                               ([ENS_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                               )
                                                         VALUES
                                                               (@ENS_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@ENS_SEQ";
                                    pparam1.Value = g_ens_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 1;
                                    g_ext_no = 1;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = ens_minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = ens_minps;  //최초 계약일+15년= 연장시작일
                                    ext_date.AddYears(15);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(15); //연장시작일 +15년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = ens_minps;
                                    scomdst.Parameters.Add(pparam12);


                                   // try
                                    //{
                                        int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }
                                    // }
                                    // catch (Exception ex)
                                    // {

                                    // }

                                }
                            }
                        }
                        else if (span2 == 45)
                        {

                            if (ext_cnt == 3)
                            {
                                // 연장정보가 3건이면 15+15+15} 추가는 15,15
                                //연장계약 15년
                                {
                                    scomdst.Parameters.Clear();
                                    scomdst.CommandText = @"INSERT INTO [dbo].[ensext]
                                                               ([ENS_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                               )
                                                         VALUES
                                                               (@ENS_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                               
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@ENS_SEQ";
                                    pparam1.Value = g_ens_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 1;
                                    g_ext_no = 1;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = ens_minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = ens_minps;  //최초 계약일+15년= 연장시작일
                                    ext_date.AddYears(15);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(15); //연장시작일 +15년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = ens_minps;
                                    scomdst.Parameters.Add(pparam12);
                                    int rows = scomdst.ExecuteNonQuery();

                                }
                                //연장계약 15년 두번째
                                {
                                    scomdst.Parameters.Clear();
                                    scomdst.CommandText = @"INSERT INTO [dbo].[ensext]
                                                               ([ENS_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE])
                                                         VALUES
                                                               (@ENS_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                                    )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@ENS_SEQ";
                                    pparam1.Value = g_ens_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 2;
                                    g_ext_no = 2;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = ens_minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = ens_minps;  //최초 계약일+30년= 연장시작일
                                    ext_date.AddYears(30);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(15); //연장시작일 +15년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam11);

                                    SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@RECEIPT_DATE";
                                    pparam12.Value = ens_minps.AddYears(15);
                                    scomdst.Parameters.Add(pparam12);
                                    int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }
                                }

                            }
                            scomdst.Parameters.Clear();
                            if (ext_cnt == 2)
                            {
                                // 연장정보가 2건이면 15+30} 추가는 30 
                                //연장계약 30년
                                {
                                    scomdst.CommandText = @"INSERT INTO [dbo].[ensext]
                                                               ([ENS_SEQ]
                                                               ,[EXT_CNT]
                                                               ,[EXT_DATE]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                              )
                                                         VALUES
                                                               (@ENS_SEQ,
                                                                @EXT_CNT, 
                                                                @EXT_DATE,
                                                                @EXT_STR_DATE,
                                                                @EXT_END_DATE, 
                                                                @DC_GUBUN,
                                                                @USE_ORI_AMT,
                                                                @USE_DC_AMT,
                                                                @USE_AMT,
                                                                @MGMT_AMT,
                                                                @RECEIPT_DATE
                                                                )";
                                    SqlParameter pparam1 = new SqlParameter();
                                    pparam1.ParameterName = "@ENS_SEQ";
                                    pparam1.Value = g_ens_seq;
                                    scomdst.Parameters.Add(pparam1);

                                    SqlParameter pparam2 = new SqlParameter();
                                    pparam2.ParameterName = "@EXT_CNT";
                                    pparam2.Value = 1;
                                    g_ext_no = 1;
                                    scomdst.Parameters.Add(pparam2);

                                    SqlParameter pparam3 = new SqlParameter();
                                    pparam3.ParameterName = "@EXT_DATE";
                                    try
                                    {
                                        pparam3.Value = ens_minps;
                                    }
                                    catch (Exception ex)
                                    {
                                        pparam3.Value = "";
                                    }
                                    scomdst.Parameters.Add(pparam3);

                                    SqlParameter pparam4 = new SqlParameter();
                                    pparam4.ParameterName = "@EXT_STR_DATE";
                                    DateTime ext_date = ens_minps;  //최초 계약일+15년= 연장시작일
                                    ext_date.AddYears(15);
                                    pparam4.Value = ext_date;
                                    scomdst.Parameters.Add(pparam4);

                                    ext_date.AddYears(30); //연장시작일 +30년= 연장종료일

                                    SqlParameter pparam5 = new SqlParameter();
                                    pparam5.ParameterName = "@EXT_END_DATE";
                                    pparam5.Value = ext_date;
                                    scomdst.Parameters.Add(pparam5);


                                    SqlParameter pparam7 = new SqlParameter();
                                    pparam7.ParameterName = "@DC_GUBUN";
                                    pparam7.Value = "TCM1200001";
                                    scomdst.Parameters.Add(pparam7);

                                    SqlParameter pparam8 = new SqlParameter();
                                    pparam8.ParameterName = "@USE_ORI_AMT";
                                    pparam8.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam8);

                                    SqlParameter pparam9 = new SqlParameter();
                                    pparam9.ParameterName = "@USE_DC_AMT";
                                    pparam9.Value = 0;
                                    scomdst.Parameters.Add(pparam9);

                                    SqlParameter pparam10 = new SqlParameter();
                                    pparam10.ParameterName = "@USE_AMT";
                                    pparam10.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam10);

                                    SqlParameter pparam11 = new SqlParameter();
                                    pparam11.ParameterName = "@MGMT_AMT";
                                    pparam11.Value = ens_pcost / 2;
                                    scomdst.Parameters.Add(pparam11);

                                   /* SqlParameter pparam12 = new SqlParameter();
                                    pparam12.ParameterName = "@EXT_CNT";
                                    pparam12.Value = ext_cnt;
                                    scomdst.Parameters.Add(pparam12);
                                    */
                                    SqlParameter pparam13 = new SqlParameter();
                                    pparam13.ParameterName = "@RECEIPT_DATE";
                                    pparam13.Value = ens_minps;
                                    scomdst.Parameters.Add(pparam13);
                                    int rows = scomdst.ExecuteNonQuery();
                                    {
                                        String query;
                                        query = scomdst.CommandText;
                                        foreach (SqlParameter parm in scomdst.Parameters)
                                        {
                                            // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                            query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                        }

                                        ResultLog.LogWrite(query);
                                    }

                                }
                            }
                        }
                        try
                        {
                            str_date = DateTime.Parse(sdrorg["ydate"].ToString());
                        }
                        catch (Exception ex)
                        {
                            str_date = DateTime.Now;
                        }
                        //enshrine insert  
                        {
                            scomdst.Parameters.Clear();
                            scomdst.CommandText = @"INSERT INTO [dbo].[enshrine]
                                                               ([FAC_ID]
                                                               ,[ENS_TYPE]
                                                               ,[ENS_DATE]
                                                               ,[LOC_CODE]
                                                               ,[ROOM_CODE]
                                                               ,[ROW_NO]
                                                               ,[COL_NO]
                                                               ,[ENS_NO]
                                                               ,[APPL_ID]
                                                               ,[STR_DATE]
                                                               ,[END_DATE]
                                                               ,[EXT_CNT]
                                                               ,[EXT_STR_DATE]
                                                               ,[EXT_END_DATE]
                                                               ,[USE_GUBUN]
                                                               ,[DC_GUBUN]
                                                               ,[USE_ORI_AMT]
                                                               ,[USE_DC_AMT]
                                                               ,[USE_AMT]
                                                               ,[MGMT_AMT]
                                                               ,[RECEIPT_DATE]
                                                               ,[MNO]
                                                                
                                                                ,[REMARK])
                                                         VALUES
                                                               (@FAC_ID, 
                                                               @ENS_TYPE, 
                                                               @ENS_DATE, 
                                                               @LOC_CODE, 
                                                               '',
                                                               @ROW_NO, 
                                                               @COL_NO, 
                                                               @ENS_NO, 
                                                               @APPL_ID, 
                                                               @STR_DATE,
                                                               @END_DATE,
                                                               @EXT_CNT,
                                                               @EXT_STR_DATE,
                                                               @EXT_END_DATE,
                                                               @USE_GUBUN, 
                                                               @DC_GUBUN, 
                                                               @USE_ORI_AMT, 
                                                               @USE_DC_AMT, 
                                                               @USE_AMT, 
                                                               @MGMT_AMT, 
                                                               @RECEIPT_DATE, 
                                                               @MNO,
                                                                @REMARK)";
                            SqlParameter pparam1 = new SqlParameter();
                            pparam1.ParameterName = "@FAC_ID";
                            pparam1.Value = fac_id;
                            scomdst.Parameters.Add(pparam1);

                            SqlParameter pparam2 = new SqlParameter();
                            pparam2.ParameterName = "@ENS_TYPE";
                            pparam2.Value = bury_type;
                            scomdst.Parameters.Add(pparam2);

                            SqlParameter pparam3 = new SqlParameter();
                            pparam3.ParameterName = "@ENS_DATE";
                            pparam3.Value = bury_date;
                            scomdst.Parameters.Add(pparam3);


                            SqlParameter pparam8 = new SqlParameter();
                            pparam8.ParameterName = "@ROW_NO";
                            pparam8.Value = 0;
                            scomdst.Parameters.Add(pparam8);

                            SqlParameter pparam9 = new SqlParameter();
                            pparam9.ParameterName = "@COL_NO";
                            pparam9.Value = 0;
                            scomdst.Parameters.Add(pparam9);


                            SqlParameter pparam11 = new SqlParameter();
                            pparam11.ParameterName = "@APPL_ID";
                            pparam11.Value = g_appl_seq;
                            scomdst.Parameters.Add(pparam11);

                            SqlParameter pparam12 = new SqlParameter();
                            pparam12.ParameterName = "@STR_DATE";
                            pparam12.Value = str_date;
                            scomdst.Parameters.Add(pparam12);

                            SqlParameter pparam12_2 = new SqlParameter();
                            pparam12_2.ParameterName = "@END_DATE";
                            pparam12_2.Value = end_date;
                            scomdst.Parameters.Add(pparam12_2);

                            SqlParameter pparam13 = new SqlParameter();
                            pparam13.ParameterName = "@EXT_STR_DATE";
                            if (ext_str_date != DateTime.Parse("2999-01-01"))
                                pparam13.Value = ext_str_date;
                            else
                                pparam13.Value = DBNull.Value;
                            scomdst.Parameters.Add(pparam13);

                            SqlParameter pparam14 = new SqlParameter();
                            pparam14.ParameterName = "@EXT_END_DATE";
                            if (ext_end_date != DateTime.Parse("1900-01-01"))
                                pparam14.Value = ext_end_date;
                            else
                                pparam14.Value = DBNull.Value; 
                            scomdst.Parameters.Add(pparam14);

                            SqlParameter pparam15 = new SqlParameter();
                            pparam15.ParameterName = "@EXT_CNT";
                            ext_cnt=0;
                            if (ext_cnt >= 2) ext_cnt = 1;
                            pparam15.Value = g_ext_no;
                            scomdst.Parameters.Add(pparam15);

                            SqlParameter pparam16 = new SqlParameter();
                            pparam16.ParameterName = "@USE_GUBUN";
                            use_gubun = "U";
                            switch (sdrorg["gtype"])
                            {
                                case 0:
                                    use_gubun = "U";
                                    break;
                                case 2:
                                    use_gubun = "B";
                                    break;
                                case 4:
                                    use_gubun = "X";
                                    break;
                            }
                            pparam16.Value = use_gubun;
                            scomdst.Parameters.Add(pparam16);

                            SqlParameter pparam17 = new SqlParameter();
                            pparam17.ParameterName = "@DC_GUBUN";
                            pparam17.Value = "TCM1200001";
                            scomdst.Parameters.Add(pparam17);

                            SqlParameter pparam18 = new SqlParameter();
                            pparam18.ParameterName = "@USE_ORI_AMT";
                            pparam18.Value = sdrorg["usecost"];
                            scomdst.Parameters.Add(pparam18);

                            SqlParameter pparam19 = new SqlParameter();
                            pparam19.ParameterName = "@USE_DC_AMT";
                            pparam19.Value = 0;
                            scomdst.Parameters.Add(pparam19);

                            SqlParameter pparam20 = new SqlParameter();
                            pparam20.ParameterName = "@USE_AMT";
                            // if (Int32.Parse(sdrorg2["usecost"].ToString()) != 0)
                            pparam20.Value = sdrorg["usecost"];
                            //else
                            switch(sdrorg["usecost"])
                                {
                                case 0:
                                    pparam20.Value = ens_pcost;
                                    break;
                                default:
                                    break;
                            }
                                pparam20.Value = t_use_amt;
                            scomdst.Parameters.Add(pparam20);

                            SqlParameter pparam21 = new SqlParameter();
                            pparam21.ParameterName = "@MGMT_AMT";
                            pparam21.Value = t_mgnt_amt;
                            scomdst.Parameters.Add(pparam21);




                            SqlParameter pparam23 = new SqlParameter();
                            pparam23.ParameterName = "@RECEIPT_DATE";
                            pparam23.Value = receipt_date;
                            scomdst.Parameters.Add(pparam23);

                            SqlParameter pparam24 = new SqlParameter();
                            pparam24.ParameterName = "@MADECOMP";
                            pparam24.Value = sdrorg["madecomp"];
                            scomdst.Parameters.Add(pparam24);


                            SqlParameter pparam25 = new SqlParameter();
                            pparam25.ParameterName = "@MNO";
                            pparam25.Value = sdrorg["mno"];
                            scomdst.Parameters.Add(pparam25);

                            SqlParameter pparam26 = new SqlParameter();
                            pparam26.ParameterName = "@LOC_CODE";
                            pparam26.Value = loc_code;
                            scomdst.Parameters.Add(pparam26);

                            SqlParameter pparam27 = new SqlParameter();
                            pparam27.ParameterName = "@DAN_NO";
                            pparam27.Value = dan_no;
                            scomdst.Parameters.Add(pparam27);

                            SqlParameter pparam28 = new SqlParameter();
                            pparam28.ParameterName = "@ENS_NO";
                            pparam28.Value = bury_no;
                            scomdst.Parameters.Add(pparam28);

                            SqlParameter pparam29 = new SqlParameter();
                            pparam29.ParameterName = "@REMARK";
                            if (sdrorg["etc"].ToString().Length > 200)
                                pparam29.Value = sdrorg["etc"].ToString().Substring(0, 200);
                            else
                                pparam29.Value = sdrorg["etc"];
                            scomdst.Parameters.Add(pparam29);
                            int rows = scomdst.ExecuteNonQuery();
                            {
                                String query;
                                query = scomdst.CommandText;
                                foreach (SqlParameter parm in scomdst.Parameters)
                                {
                                    // ResultLog.LogWrite("[" + parm.ParameterName + "]===>" + parm.Value.ToString());
                                    query = query.Replace(parm.ParameterName, parm.Value.ToString());

                                }

                                ResultLog.LogWrite(query);
                            }

                        }
                        //sconorg.Open();
                        {

                        }
                        g_ens_seq++;
                        break;
                    case "8":
                        //8.자연장지 는 nature(자연장계약정보) 로 이관 대상 테이블을 설정
                        bury_type = "TFM0400001";
                        bury_kind = "TFM0500001";
                        //안치형태  자연 
                        String nattype = "";
                        nattype = sdrorg["mptype"].ToString();
                        switch (nattype)
                        {
                            case "0":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500001";
                                break;
                            case "1":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500002";
                                break;
                            case "2":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500001";
                                break;
                            case "3":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500001";
                                break;
                            case "4":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500001";
                                break;
                            case "5":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500002";
                                break;
                            case "6":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500002";
                                break;
                            case "7":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500001";
                                break;
                            case "8":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500001";
                                break;
                            case "9":
                                bury_type = "TFM1600003";
                                bury_kind = "TFM0500001";
                                break;
                        }
                        switch (mno_code[1]) //구역번호 처리 
                        {
                            case "가":
                                loc_code = "01";
                                break;
                            case "나":
                                loc_code = "02";
                                break;
                            default:
                                loc_code = mno_code[1];
                                break;

                        }


                        try
                        {
                            bury_no = Int32.Parse(mno_code[3]);
                        }
                        catch (Exception ex)
                        {
                            bury_no = 0;
                        }
                        try
                        {


                            bury_no2 = Int32.Parse(mno_code[4]);
                        }
                        catch (Exception ex)
                        {
                            bury_no2 = 0;
                        }
                        fac_id = mno_code[0];
                        //bury_type = "TFM0400001";
                        //bury_kind = "TFM0500001";
                        receipt_date = DateTime.Now;
                        //행,열 번호 초기화 
                        row_no = 0;
                        col_no = 0;
                        span = 0;
                        // 안치형태 
                        //bury_type;
                        //합장구분
                        //bury_kind
                        //계약일자
                        bury_date = DateTime.Parse(sdrorg["ydate"].ToString());
                        //설치일자 
                        set_date = DateTime.Parse(sdrorg["setdate"].ToString());
                        //만기일자 
                        end_date = DateTime.Parse(sdrorg["expdate"].ToString());
                        //계약기간 계산 
                        span = end_date.Year - bury_date.Year;
                        ext_cnt = 0;
                        scomorg2.Parameters.Clear();
                        bury_date = DateTime.Parse(sdrorg["ydate"].ToString());
                        //설치일자 
                        set_date = DateTime.Parse(sdrorg["setdate"].ToString());
                        //만기일자 
                        end_date = DateTime.Parse(sdrorg["expdate"].ToString());
                        //계약기간 계산 
                        span = end_date.Year - bury_date.Year;
                        ext_cnt = 0;
                        scomorg2.Parameters.Clear();
                        // tb_cust_damo 에서 계약자 정보 꺼냄
                        scomorg2.CommandText = @"SELECT TOP (1000) [seq]
                                                  ,[unamea]
                                                  ,[jumin]
                                                  ,[tel]
                                                  ,[hp]
                                                  ,[email]
                                                  ,[zip]
                                                  ,[addr]
                                                  ,[related]
                                              FROM [dangjin].[dbo].[tb_cust]
                                              where [seq]=@seq_cust";
                        SqlParameter nat_param = new SqlParameter();
                        nat_param.ParameterName = "@seq_cust";
                        nat_param.Value = sdrorg["seq_customer"];
                        scomorg2.Parameters.Add(nat_param);
                        sdrorg2 = scomorg2.ExecuteReader();
                        //계약자 정보 이전  
                        g_appl_seq = 0;
                        while (sdrorg2.Read())
                        {
                            //  g_appl_seq++;
                            scomdst.CommandText = @"INSERT INTO [dbo].[applicant]
                                                   ([APPL_NAME]
                                                   ,[APPL_JUMIN]
                                                   ,[TELNO1]
                                                   ,[TELNO2]
                                                   ,[TELNO3]
                                                   ,[MOBILENO1]
                                                   ,[MOBILENO2]
                                                   ,[MOBILENO3]
                                                   ,[ADDR_GUBUN]
                                                   ,[APPL_POST]
                                                   ,[APPL_ADDR1]
                                                   ,[APPL_ADDR2]
                                                   ,[APPL_REMARK])
                                             VALUES
                                                   (@APPL_NAME
                                                   ,@APPL_JUMIN
                                                   ,@TELNO1
                                                   ,@TELNO2
                                                   ,@TELNO3
                                                   ,@MOBILENO1
                                                   ,@MOBILENO2
                                                   ,@MOBILENO3
                                                   ,@ADDR_GUBUN
                                                   ,@APPL_POST
                                                   ,@APPL_ADDR1
                                                   ,'   ','   ')";

                            string[] tel_no = sdrorg2["tel"].ToString().Split('-');
                            string[] hp_no = sdrorg2["hp"].ToString().Split('-');
                            //SqlParameter param0 = new SqlParameter();
                            //param0.ParameterName = "@APPL_ID";
                            //param0.Value = g_appl_seq;
                            //scomdst.Parameters.Add(param0);
                            SqlParameter param1 = new SqlParameter();
                            param1.ParameterName = "@APPL_NAME";
                            param1.Value = sdrorg2["unamea"];
                            scomdst.Parameters.Add(param1);
                            SqlParameter param2 = new SqlParameter();
                            param2.ParameterName = "@APPL_JUMIN";
                            param2.Value = sdrorg2["jumin"];
                            scomdst.Parameters.Add(param2);
                            SqlParameter param3 = new SqlParameter();
                            param3.ParameterName = "@TELNO1";

                            if (tel_no.Length < 3)
                                param3.Value = "041";
                            else
                                param3.Value = tel_no[0];

                            SqlParameter param3_2 = new SqlParameter();
                            param3_2.ParameterName = "@TELNO2";
                            if (tel_no.Length < 3)
                                param3_2.Value = tel_no[0];
                            else
                                param3_2.Value = tel_no[1];
                            scomdst.Parameters.Add(param3_2);
                            SqlParameter param3_3 = new SqlParameter();
                            param3_3.ParameterName = "@TELNO3";
                            if (tel_no.Length > 1)
                            {
                                if (tel_no.Length < 3)
                                    param3_3.Value = tel_no[0];
                                else
                                    param3_3.Value = tel_no[2];
                            }
                            else param3_3.Value = "";
                            if (tel_no.Length <= 1)
                            {
                                param3.Value = "";
                                param3_2.Value = "";
                                param3_3.Value = "";

                            }
                            if (param3_2.Value.Equals("")) param3.Value = "";
                            scomdst.Parameters.Add(param3);
                            scomdst.Parameters.Add(param3_3);
                            SqlParameter param4 = new SqlParameter();
                            param4.ParameterName = "@MOBILENO1";
                            if (hp_no.Length > 0)
                                param4.Value = hp_no[0];
                            else param4.Value = " ";
                            scomdst.Parameters.Add(param4);
                            SqlParameter param4_2 = new SqlParameter();
                            param4_2.ParameterName = "@MOBILENO2";
                            if (hp_no.Length > 1)
                                param4_2.Value = hp_no[1];
                            else
                                param4_2.Value = " ";
                            scomdst.Parameters.Add(param4_2);
                            SqlParameter param4_3 = new SqlParameter();
                            param4_3.ParameterName = "@MOBILENO3";
                            if (hp_no.Length > 2)
                                param4_3.Value = hp_no[2];
                            else
                                param4_3.Value = " ";
                            scomdst.Parameters.Add(param4_3);
                            SqlParameter param5 = new SqlParameter();
                            param5.ParameterName = "@ADDR_GUBUN";
                            param5.Value = "TCM0700001";
                            scomdst.Parameters.Add(param5);
                            SqlParameter param6 = new SqlParameter();
                            param6.ParameterName = "@APPL_POST";
                            string str1 = sdrorg2["zip"].ToString();
                            string str2 = str1.Replace("-", "");
                            param6.Value = str2;
                            scomdst.Parameters.Add(param6);
                            SqlParameter param7 = new SqlParameter();
                            param7.ParameterName = "@APPL_ADDR1";
                            param7.Value = sdrorg2["addr"];
                            scomdst.Parameters.Add(param7);
                            SqlParameter param8 = new SqlParameter();
                            param8.ParameterName = "@APPL_REMARK";
                            param8.Value = sdrorg2["related"];
                            scomdst.Parameters.Add(param8);

                            // 계약자가  이미 들어 있으면 가져옴 
                            g_appl_seq = Int32.Parse(get_APP_ID(param1.Value.ToString(), param3.Value.ToString(), param3_2.Value.ToString(), param3_3.Value.ToString(), param6.Value.ToString()));
                            //int rows = scomdst.ExecuteNonQuery();
                            //계약자 ID 조회 
                            if (g_appl_seq == -1)
                            {
                                scomdst.CommandText = @"SELECT MAX([APPL_ID])  FROM [funeralsystem_dangjin].[dbo].[applicant]";

                                g_appl_seq = (Int32)scomdst.ExecuteScalar();
                            }
                        }
                        sdrorg2.Close();

                        // seq_moyji로  고인정보에서 고인정보 추출하여 thedead_damo와 naturedead 저장 
                        scomorg2.CommandText = @"SELECT TOP (1000) [seq]
                                              ,[seq_myoji]
                                              ,[unameb]
                                              ,[mdate]
                                              ,[juminb]
                                              ,[sleepdate]
                                              ,[addrb]
                                              ,[sleepwhy]
                                              ,[sleepon]
                                              ,[age]
                                              ,[btype]
                                              ,[related2]
                                              ,[istop]
                                          FROM [dangjin].[dbo].[tb_bong]
                                          where seq_myoji=@seq_myoji2";

                        SqlParameter nat_param9 = new SqlParameter();
                        nat_param9.ParameterName = "@seq_myoji2";
                        nat_param9.Value = sdrorg[0];
                        scomorg2.Parameters.Add(nat_param9);
                        sdrorg2 = scomorg2.ExecuteReader();

                        int nat_sunbun = 1;//고인순번
                        g_dead_seq = 0;
                        //고인정보가 존재하면 

                        while (sdrorg2.Read())
                        {

                            // 고인정보에 이전 thedead
                            scomdst.CommandText = @"INSERT INTO [dbo].[thedead]
                            ([DEAD_NAME]
                            ,[DEAD_JUMIN]
                            ,[DEAD_AGE]
                            ,[DEAD_DATE]
                            ,[DEAD_PLACE_TXT]
                            ,[DEAD_REASON_TXT]
                            ,[OBJT]
                            ,[ADDR_GUBUN]
                             ,[DEAD_ADDR1]
                             ,[DEAD_ADDR2]
                            )
                        VALUES
                            (@DEAD_NAME
                            ,@DEAD_JUMIN
                            ,@DEAD_AGE
                            ,@DEAD_DATE
                            ,@DEAD_PLACE_TXT
                            ,@DEAD_REASON_TXT
                            ,@OBJT
                            ,@ADDR_GUBUN2
                            ,@DEAD_ADDR1
                            ,'  '
                            )";

                            scomdst.Parameters.Clear();
                            SqlParameter param1 = new SqlParameter();
                            param1.ParameterName = "@DEAD_NAME";
                            param1.Value = sdrorg2["unameb"].ToString();
                            scomdst.Parameters.Add(param1);
                            SqlParameter param2 = new SqlParameter();
                            param2.ParameterName = "@DEAD_JUMIN";
                            param2.Value = sdrorg2["juminb"];
                            scomdst.Parameters.Add(param2);



                            SqlParameter param4 = new SqlParameter();
                            param4.ParameterName = "@DEAD_AGE";
                            param4.Value = sdrorg2["age"];
                            scomdst.Parameters.Add(param4);

                            SqlParameter param5 = new SqlParameter();
                            param5.ParameterName = "@DEAD_DATE";
                            try
                            {
                                param5.Value = DateTime.Parse(sdrorg2["sleepdate"].ToString());
                            }
                            catch (Exception ex)
                            {
                                param5.Value = DBNull.Value;
                            }
                            scomdst.Parameters.Add(param5);

                            SqlParameter param6 = new SqlParameter();
                            param6.ParameterName = "@DEAD_PLACE_TXT";
                            param6.Value = sdrorg2["sleepon"].ToString();
                            scomdst.Parameters.Add(param6);

                            SqlParameter param8 = new SqlParameter();
                            param8.ParameterName = "@DEAD_REASON_TXT";
                            param8.Value = sdrorg2["sleepwhy"].ToString();
                            scomdst.Parameters.Add(param8);

                            SqlParameter param8_2 = new SqlParameter();
                            param8_2.ParameterName = "@OBJT";
                            mptype = sdrorg["mtype"].ToString();
                            switch (mptype)
                            {
                                case "0"://분묘단장
                                    param8_2.Value = "TFM0100001";
                                    break;
                                case "1"://분묘합장
                                    param8_2.Value = "TFM0100001";
                                    break;
                                default:
                                    param8_2.Value = "TFM0100002";
                                    break;
                            }
                            scomdst.Parameters.Add(param8_2);

                            SqlParameter param8_3 = new SqlParameter();
                            param8_3.ParameterName = "@ADDR_GUBUN2";
                            param8_3.Value = "TCM0700001";
                            scomdst.Parameters.Add(param8_3);

                            SqlParameter param8_4 = new SqlParameter();
                            param8_4.ParameterName = "@DEAD_POST";
                            param8_4.Value = " ";
                            scomdst.Parameters.Add(param8_4);

                            SqlParameter param8_5 = new SqlParameter();
                            param8_5.ParameterName = "@DEAD_ADDR1";
                            param8_5.Value = sdrorg2["addrb"].ToString();
                            scomdst.Parameters.Add(param8_5);
                            try
                            {
                                int ret = scomdst.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("{0} DATA FAILED", g_trans_rec);
                                ResultLog.LogWrite("2: "+ex.Message);
                                string query = scomdst.CommandText;

                                foreach ( SqlParameter parm in scomdst.Parameters)
                                {
                                  //  ResultLog.LogWrite("["+parm.ParameterName+"]===>"+parm.Value.ToString());
                                    query = query.Replace(parm.ParameterName, parm.Value.ToString());
                                  //  ResultLog.LogWrite(query);
                                }
                                ResultLog.LogWrite(query);
                            }
                            String retstr = get_DEAD_ID(param1.Value.ToString(), param4.Value.ToString(), param5.Value.ToString());
                            if (String.IsNullOrEmpty(retstr)) g_dead_seq = -1;
                            else g_dead_seq = Int32.Parse(retstr);
                           
                            if(g_dead_seq == -1)
                            {
                                scomdst.CommandText = @"SELECT MAX([DEAD_ID])  FROM [funeralsystem_dangjin].[dbo].[thedead]";

                                g_dead_seq = (Int32)scomdst.ExecuteScalar();
                            }
                            scomdst.Parameters.Clear();
                            //--자연장계약별고인정보 --
                            scomdst.CommandText = @"INSERT INTO [dbo].[naturedead]
                                                       ([NAT_SEQ]
                                                       ,[DEAD_SEQ]
                                                       ,[REAL_DATE]
                                                       ,[DEAD_ID]
                                                       ,[ISTOP]
                                                       ,[DEAD_RELATION_NM]
                                                       ,[USE_STATUS]
                                                       )
                                                 VALUES
                                                       (@NAT_SEQ,  
                                                       @DEAD_SEQ,  
                                                       @REAL_DATE,  
                                                       @DEAD_ID,  
                                                       @ISTOP,  
                                                       @DEAD_RELATION_NM,  
                                                       @USE_STATUS )
                                                       ";
                            SqlParameter pparam1 = new SqlParameter();
                            pparam1.ParameterName = "@NAT_SEQ";
                            pparam1.Value = g_nature_seq;
                            scomdst.Parameters.Add(pparam1);

                            SqlParameter pparam2 = new SqlParameter();
                            pparam2.ParameterName = "@DEAD_SEQ";
                            pparam2.Value = nat_sunbun;
                            scomdst.Parameters.Add(pparam2);

                            SqlParameter pparam3 = new SqlParameter();
                            pparam3.ParameterName = "@REAL_DATE";
                            try
                            {
                                pparam3.Value = DateTime.Parse(sdrorg2["mdate"].ToString());
                            }
                            catch (Exception ex)
                            {
                                pparam3.Value = "";
                            }
                            scomdst.Parameters.Add(pparam3);

                            SqlParameter pparam4 = new SqlParameter();
                            pparam4.ParameterName = "@DEAD_ID";
                            pparam4.Value = g_dead_seq;
                            scomdst.Parameters.Add(pparam4);

                            SqlParameter pparam5 = new SqlParameter();
                            pparam5.ParameterName = "@ISTOP";
                            pparam5.Value = sdrorg2["istop"];
                            scomdst.Parameters.Add(pparam5);


                            SqlParameter pparam7 = new SqlParameter();
                            pparam7.ParameterName = "@DEAD_RELATION_NM";
                            pparam7.Value = sdrorg2["related2"];
                            scomdst.Parameters.Add(pparam7);

                            SqlParameter pparam8 = new SqlParameter();
                            pparam8.ParameterName = "@USE_STATUS";
                            pparam8.Value = 1;
                            scomdst.Parameters.Add(pparam8);

                            int rows = scomdst.ExecuteNonQuery();
                            nat_sunbun++;
                          //  g_dead_seq++;
                        }
                        sdrorg2.Close();
                        ///----------------------------------------------------------------------------------------
                        //

                        // tb_term에서 해당묘지 납부건 조회 
                        {
                            scomorg2.CommandText = @"SELECT [seq]
                                                        ,[seq_myoji]
                                                        ,[ptype]
                                                        ,[pyear]
                                                        ,[psdate]
                                                        ,[pedate]
                                                        ,[pdate]
                                                        ,[pycost]
                                                        ,[pcost]
                                                        ,[npcost]
                                                        ,[petc]
                                                    FROM [dangjin].[dbo].[tb_term]
                                                    where [seq_myoji] = @seq_myoji
                                                    ";

                            SqlParameter termparam9 = new SqlParameter();
                            termparam9.ParameterName = "@seq_myoji";
                            termparam9.Value = sdrorg[0];
                            scomorg2.Parameters.Add(termparam9);
                            sdrorg2 = scomorg2.ExecuteReader();
                        }
                        int nat_term_total = 0;
                        ext_cnt = 0;
                        //납부내역이 존재하면  필요한 정보추출 
                        DateTime nat_minps, nat_maxpe,nat_maxps;
                        int nat_pcost = 0;
                        t_mgnt_amt = 0;
                        t_use_amt = 0;
                        //가장작은 시작일자와 가장큰 종료 일자로 총기간 산정 
                        nat_minps = DateTime.Parse("2999-01-01");
                        nat_maxpe = DateTime.Parse("1900-01-01");
                        nat_maxps = DateTime.Parse("1900-01-01");
                        while (sdrorg2.Read())
                        {
                            try
                            {
                                if (nat_minps > DateTime.Parse(sdrorg2["psdate"].ToString())) nat_minps = DateTime.Parse(sdrorg2["psdate"].ToString());
                                if (nat_maxpe < DateTime.Parse(sdrorg2["pedate"].ToString())) nat_maxpe = DateTime.Parse(sdrorg2["pedate"].ToString());
                                if (nat_maxps < DateTime.Parse(sdrorg2["pedate"].ToString())) nat_maxps = DateTime.Parse(sdrorg2["pedate"].ToString());
                            }
                            catch (Exception ex)
                            {
                                if (DateTime.Parse("1800-01-01") != nat_minps)
                                    nat_minps = nat_minps;
                                else
                                    ens_minps = DateTime.Parse("1800-01-01");
                                if (DateTime.Parse("1800-01-01") != nat_maxpe)
                                    nat_maxpe = nat_maxpe;
                                else
                                    nat_maxpe = DateTime.Parse("1800-01-01");
                            }
                            nat_pcost += Int32.Parse(sdrorg2["pcost"].ToString());
                            try
                            {
                                switch (Int32.Parse(sdrorg2["ptype"].ToString()))
                                {
                                    case 0:
                                        t_mgnt_amt += Int32.Parse(sdrorg2["pcost"].ToString());
                                        break;
                                    case 1:
                                        t_use_amt += Int32.Parse(sdrorg2["pcost"].ToString());
                                        break;

                                }
                            }
                            catch (Exception ex)
                            {
                                int err = 1;
                            }
                            receipt_date = DateTime.Now;
                            try
                            {
                                receipt_date = DateTime.Parse(sdrorg2["pdate"].ToString());
                            }
                            catch (Exception ex)
                            {
                                receipt_date = nat_maxps;
                            }
                            // term_total +=DateTime.Parse(sdrorg2["pedate"].ToString()).Year - DateTime.Parse(sdrorg2["psdate"].ToString()).Year;
                            ext_cnt++;
                        }
                        sdrorg2.Close();
                        ext_str_date = nat_minps;
                        ext_end_date = nat_maxpe;
                        ens_term_total = nat_maxpe.Year - nat_minps.Year;
                        str_date = DateTime.Parse(sdrorg["ydate"].ToString());
                        //nature insert  
                        {
                            scomdst.Parameters.Clear();
                            scomdst.CommandText = @"INSERT INTO [dbo].[nature]
                           ([FAC_ID]
                           ,[NAT_TYPE]
                           ,[NAT_DATE]
                           ,[NAT_KIND]
                           ,[LOC_CODE]
                           ,[DAN_NO]
                           ,[ROW_NO]
                           ,[COL_NO]
                           ,[NAT_NO]
                           ,[APPL_ID]
                           ,[STR_DATE]
                           ,[END_DATE]
                           ,[USE_GUBUN]
                           ,[DC_GUBUN]
                           ,[USE_ORI_AMT]
                           ,[USE_DC_AMT]
                           ,[USE_AMT]
                           ,[MGMT_AMT]
                           ,[GRASS_AMT]
                           ,[RECEIPT_DATE],[MNO],[REMARK])
                     VALUES
                           (@FAC_ID, 
                           @NAT_TYPE,
                           @NAT_DATE,
                           @NAT_KIND,
                           @LOC_CODE,
                           @DAN_NO, 
                           @ROW_NO, 
                           @COL_NO,  
                           @NAT_NO,  
                           @APPL_ID, 
                           @STR_DATE,  
                           @END_DATE,  
                           @USE_GUBUN,  
                           @DC_GUBUN,  
                           @USE_ORI_AMT,  
                           @USE_DC_AMT,  
                           @USE_AMT,  
                           @MGMT_AMT,  
                           @GRASS_AMT,  
                           @RECEIPT_DATE,
                           @MNO,@REMARK)";
                            SqlParameter pparam1 = new SqlParameter();
                            pparam1.ParameterName = "@FAC_ID";
                            pparam1.Value = fac_id;
                            scomdst.Parameters.Add(pparam1);

                            SqlParameter pparam2 = new SqlParameter();
                            pparam2.ParameterName = "@NAT_TYPE";
                            pparam2.Value = bury_type;
                            scomdst.Parameters.Add(pparam2);

                            SqlParameter pparam3 = new SqlParameter();
                            pparam3.ParameterName = "@NAT_DATE";
                            pparam3.Value = bury_date;
                            scomdst.Parameters.Add(pparam3);


                            SqlParameter pparam8 = new SqlParameter();
                            pparam8.ParameterName = "@ROW_NO";
                            pparam8.Value = 0;
                            scomdst.Parameters.Add(pparam8);

                            SqlParameter pparam9 = new SqlParameter();
                            pparam9.ParameterName = "@COL_NO";
                            pparam9.Value = 0;
                            scomdst.Parameters.Add(pparam9);


                            SqlParameter pparam11 = new SqlParameter();
                            pparam11.ParameterName = "@APPL_ID";
                            pparam11.Value = g_appl_seq;
                            scomdst.Parameters.Add(pparam11);

                            SqlParameter pparam12 = new SqlParameter();
                            pparam12.ParameterName = "@STR_DATE";
                            pparam12.Value = str_date;
                            scomdst.Parameters.Add(pparam12);

                            SqlParameter pparam12_2 = new SqlParameter();
                            pparam12_2.ParameterName = "@END_DATE";
                            pparam12_2.Value = end_date;
                            scomdst.Parameters.Add(pparam12_2);

                         /*   SqlParameter pparam13 = new SqlParameter();
                            pparam13.ParameterName = "@EXT_STR_DATE";
                            pparam13.Value = ext_str_date;
                            scomdst.Parameters.Add(pparam13);

                            SqlParameter pparam14 = new SqlParameter();
                            pparam14.ParameterName = "@EXT_END_DATE";
                            pparam14.Value = ext_end_date;
                            scomdst.Parameters.Add(pparam14);

                            SqlParameter pparam15 = new SqlParameter();
                            pparam15.ParameterName = "@EXT_CNT";
                            pparam15.Value = ext_cnt;
                            scomdst.Parameters.Add(pparam15); */

                            SqlParameter pparam16 = new SqlParameter();
                            pparam16.ParameterName = "@USE_GUBUN";
                            use_gubun = "U";
                            switch (sdrorg["gtype"])
                            {
                                case 0:
                                    use_gubun = "U";
                                    break;
                                case 2:
                                    use_gubun = "B";
                                    break;
                                case 4:
                                    use_gubun = "X";
                                    break;
                            }
                            pparam16.Value = use_gubun;
                            scomdst.Parameters.Add(pparam16);

                            SqlParameter pparam17 = new SqlParameter();
                            pparam17.ParameterName = "@DC_GUBUN";
                            pparam17.Value = "TCM1200001";
                            scomdst.Parameters.Add(pparam17);

                            SqlParameter pparam18 = new SqlParameter();
                            pparam18.ParameterName = "@USE_ORI_AMT";
                            pparam18.Value = sdrorg["usecost"];
                            scomdst.Parameters.Add(pparam18);

                            SqlParameter pparam19 = new SqlParameter();
                            pparam19.ParameterName = "@USE_DC_AMT";
                            pparam19.Value = 0;
                            scomdst.Parameters.Add(pparam19);

                            SqlParameter pparam20 = new SqlParameter();
                            pparam20.ParameterName = "@USE_AMT";
                            // if (Int32.Parse(sdrorg2["usecost"].ToString()) != 0)
                            pparam20.Value = t_use_amt;
                            //else
                            switch(sdrorg["usecost"])
                                {
                                case 0:
                                    pparam20.Value = nat_pcost;
                                    break;
                                default:
                                    break;
                            }
                                pparam20.Value = nat_pcost;
                            scomdst.Parameters.Add(pparam20);

                            SqlParameter pparam21 = new SqlParameter();
                            pparam21.ParameterName = "@MGMT_AMT";
                            pparam21.Value = t_mgnt_amt;
                            scomdst.Parameters.Add(pparam21);




                            SqlParameter pparam23 = new SqlParameter();
                            pparam23.ParameterName = "@RECEIPT_DATE";
                            pparam23.Value = receipt_date;
                            scomdst.Parameters.Add(pparam23);

                            SqlParameter pparam26 = new SqlParameter();
                            pparam26.ParameterName = "@LOC_CODE";
                            pparam26.Value = loc_code;
                            scomdst.Parameters.Add(pparam26);

                            SqlParameter pparam27 = new SqlParameter();
                            pparam27.ParameterName = "@DAN_NO";
                            pparam27.Value = dan_no;
                            scomdst.Parameters.Add(pparam27);

                            SqlParameter pparam28 = new SqlParameter();
                            pparam28.ParameterName = "@NAT_NO";
                            pparam28.Value = bury_no;
                            scomdst.Parameters.Add(pparam28);

                            SqlParameter pparam29 = new SqlParameter();
                            pparam29.ParameterName = "@GRASS_AMT";
                            pparam29.Value = sdrorg["jancost"];
                            scomdst.Parameters.Add(pparam29);

                            SqlParameter pparam30 = new SqlParameter();
                            pparam30.ParameterName = "@MNO";
                            pparam30.Value = sdrorg["mno"];
                            scomdst.Parameters.Add(pparam30);

                            SqlParameter pparam31 = new SqlParameter();
                            pparam31.ParameterName = "@REMARK";
                            if (sdrorg["etc"].ToString().Length > 200)
                                pparam31.Value = sdrorg["etc"].ToString().Substring(0, 200).ToString().Substring(0, 200);
                            else
                                pparam31.Value = sdrorg["etc"];
                            scomdst.Parameters.Add(pparam31);

                            SqlParameter pparam7 = new SqlParameter();
                            pparam7.ParameterName = "@NAT_KIND";
                            pparam7.Value = "TFM0500001";
                            scomdst.Parameters.Add(pparam7);
                            int rows = scomdst.ExecuteNonQuery();

                        }
                        //sconorg.Open();
                        {

                        }
                        g_nature_seq++;
                        break;



                }
                int k = 0;
                g_trans_rec++;
                if (g_trans_rec ==6944)
                {
                    k = 1;
                }
                Console.WriteLine("{0} RECORDS TRANSFERED",g_trans_rec);
                if (g_appl_seq==1220)
                {
                    k = 2;
                }
            }
            sdrorg.Close();
            // new DB 주소 2 update 
            scomdst.CommandText = @"update  applicant set [APPL_ADDR2]='  ';
                                   update thedead set [DEAD_ADDR2]='   ';";

            scomdst.ExecuteNonQuery();
            Console.WriteLine("완료...");
            Console.WriteLine("{0} ms Elapsed", sw.ElapsedMilliseconds);
            Console.WriteLine("Press 'S' key to close");
            Console.ReadKey(true);
        }

    }


    internal class loc
    {
        public int row;
        public int col;
        public loc(int r,int c)
        {
            row = r;
            col =c;
        }
    }
    public class LogWriter
    {
        private string m_exePath = string.Empty;
        public LogWriter(string logMessage)
        {
            LogWrite(logMessage);
        }
        public void LogWrite(string logMessage)
        {
            m_exePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            try
            {
                using (StreamWriter w = File.AppendText(m_exePath + "\\" + "log.txt"))
                {
                    Log(logMessage, w);
                }
            }
            catch (Exception ex)
            {
            }
        }

        public void Log(string logMessage, TextWriter txtWriter)
        {
            try
            {
               // txtWriter.Write("\r\nLog Entry : ");
                txtWriter.Write("{0} {1}:", DateTime.Now.ToLongTimeString(),
                    DateTime.Now.ToLongDateString());
                //txtWriter.WriteLine("  :");
                txtWriter.WriteLine("  :{0}", logMessage);
               // txtWriter.WriteLine("-------------------------------");
            }
            catch (Exception ex)
            {
            }
        }
    }
}

