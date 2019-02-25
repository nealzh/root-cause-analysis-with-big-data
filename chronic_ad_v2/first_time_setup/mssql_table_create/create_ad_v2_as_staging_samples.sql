USE [F10DS_SOLUTION]
GO

/****** Object:  Table [dbo].[ad_v2t_as_staging_samples]    Script Date: 12/24/2018 5:09:13 PM ******/
DROP TABLE [dbo].[ad_v2_as_staging_samples]
GO

/****** Object:  Table [dbo].[ad_v2t_as_staging_samples]    Script Date: 12/24/2018 5:09:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ad_v2_as_staging_samples](
	[analysis_type] [varchar](30) NOT NULL,
	[fab] [int] NOT NULL,
	[module] [varchar](20) NULL,
	[design_id] [varchar](30) NULL,
	[ch_id] [varchar](20) NOT NULL,
	[current_step] [varchar](100) NOT NULL,
	[process_step] [varchar](100) NULL,
	[parameter_name] [varchar](200) NULL,
	[lot_id] [varchar](15) NULL,
	[wafer_id] [varchar](15) NULL,
	[limit_enable] [varchar](50) NULL,
	[sample_date] [datetime] NOT NULL,
	[sample_id] [varchar](20) NOT NULL,
	[chart_type] [varchar](20) NOT NULL,
	[value] [float] NULL,
	[ucl] [float] NULL,
	[lcl] [float] NULL,
	[col_type] [varchar](50) NULL,
	[channel_type] [varchar](50) NULL,
	[label] [int] NOT NULL,
	[violation_type] [varchar](10) NULL,
	[query_session] [datetime] NOT NULL,
PRIMARY KEY CLUSTERED
(
	[analysis_type] ASC,
	[fab] ASC,
	[ch_id] ASC,
	[chart_type] ASC,
	[query_session] ASC,
	[sample_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

