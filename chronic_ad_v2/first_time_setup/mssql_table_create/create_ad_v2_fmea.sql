USE [F10DS_SOLUTION]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ad_v2_fmea](
	[fab] [int] NOT NULL,
	[channel_id] [int] NOT NULL,
	[measurement_stepname] [varchar](200) NOT NULL,
	[process_stepname] [varchar](200) NOT NULL,
	[updated_datetime] [datetime] NOT NULL,
	PRIMARY KEY CLUSTERED
(
	[fab] ASC,
	[channel_id] ASC,
	[process_stepname] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO