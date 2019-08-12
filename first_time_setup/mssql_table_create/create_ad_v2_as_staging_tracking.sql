USE [F10DS_SOLUTION]
GO

/****** Object:  Table [dbo].[ad_v2_as_staging_tracking]    Script Date: 1/3/2019 10:35:15 AM ******/
DROP TABLE [dbo].[ad_v2_as_staging_tracking]
GO

/****** Object:  Table [dbo].[ad_v2_as_staging_tracking]    Script Date: 1/3/2019 10:35:15 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ad_v2_as_staging_tracking](
	[analysis_type] [varchar](100) NOT NULL,
	[fab] [int] NOT NULL,
	[module] [varchar](160) NULL,
	[ch_id] [varchar](100) NOT NULL,
	[chart_type] [varchar](50) NOT NULL,
	[channel_type] [varchar](100) NULL,
	[design_id] [varchar](80) NULL,
	[query_session] [datetime] NOT NULL,
	[current_step] [varchar](100) NOT NULL,
	[parameter_name] [varchar](256) NULL,
	[process_step] [varchar](100) NOT NULL,
	[ooc] [int] NULL,
	[normal] [int] NULL,
	[analysis_ad_datetime] [datetime] NULL,
	[received_ad_datetime] [datetime] NULL,
	[result_update_ad] [datetime] NULL,
	[final_status] [varchar](5000) NULL,
	[report_url] [varchar](2000) NULL,
	[updated_datetime] [datetime] NULL,
PRIMARY KEY CLUSTERED
(
	[analysis_type] ASC,
	[fab] ASC,
	[ch_id] ASC,
	[process_step] ASC,
	[current_step] ASC,
	[chart_type] ASC,
	[query_session] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

