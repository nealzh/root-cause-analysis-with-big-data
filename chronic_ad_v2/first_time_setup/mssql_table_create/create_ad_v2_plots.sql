USE [F10DS_SOLUTION]
GO

/****** Object:  Table [dbo].[ad_v2_plots]    Script Date: 2/20/2019 1:44:49 PM ******/
DROP TABLE [dbo].[ad_v2_plots]
GO

/****** Object:  Table [dbo].[ad_v2_plots]    Script Date: 2/20/2019 1:44:49 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ad_v2_plots](
	[analysis_type] [varchar](100) NOT NULL,
	[fab] [int] NOT NULL,
	[report_url] [varchar](300) NOT NULL,
	-- ooc plot or result plot
	[plot_type] [varchar](50) NOT NULL,
	-- box plot or trending plot
	[pic_type] [varchar](50) NOT NULL,
	[rank] [int] NOT NULL,
	[pic_url] [varchar](500) NULL,
	[updated_datetime] [datetime] NULL,
PRIMARY KEY CLUSTERED
(
	[report_url] ASC,
	[analysis_type] ASC,
	[fab] ASC,
	[plot_type] ASC,
	[pic_type] ASC,
	[rank] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

