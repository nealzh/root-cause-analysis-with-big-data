USE [F10DS_SOLUTION]
GO

/****** Object:  Table [dbo].[ad_v2t_root_causes]    Script Date: 12/24/2018 5:10:39 PM ******/
DROP TABLE [dbo].[ad_v2_root_causes]
GO

/****** Object:  Table [dbo].[ad_v2t_root_causes]    Script Date: 12/24/2018 5:10:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ad_v2_root_causes](
	[analysis_type] [varchar](100) NOT NULL,
	[fab] [int] NOT NULL,
	[module] [varchar](160) NULL,
	[ch_id] [varchar](50) NOT NULL,
	[chart_type] [varchar](50) NOT NULL,
	[channel_type] [varchar](100) NULL,
	[design_id] [varchar](80) NULL,
	[query_session] [datetime] NOT NULL,
	[current_step] [varchar](80) NOT NULL,
	[parameter_name] [varchar](256) NULL,
	[process_step] [varchar](80) NULL,
	[rank] [int] NOT NULL,
	[mfg_process_step] [varchar](80) NULL,
	[step_type] [varchar](50) NULL,
	[feature] [varchar](80) NULL,
	[context_value] [varchar](80) NULL,
	[Rsquare] [float] NULL,
	[reason] [varchar](100) NULL,
	[report_url] [varchar](2000) NULL,
	[updated_datetime] [datetime] NULL,
PRIMARY KEY CLUSTERED
(
	[analysis_type] ASC,
	[fab] ASC,
	[ch_id] ASC,
	[chart_type] ASC,
	[query_session] ASC,
	[rank] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

