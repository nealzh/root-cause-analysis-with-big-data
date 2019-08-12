USE [F10DS_SOLUTION]
GO

/****** Object:  Table [dbo].[AD_Feedback]    Script Date: 12/17/2018 12:25:16 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ad_v2_feedback](
  [analysis_type] [varchar](100) NOT NULL,
	[micron_user_name] [varchar](100) NOT NULL,
	[fab] [int] NOT NULL,
	[area] [varchar](100) NOT NULL,
	[session_id] [datetime] NOT NULL,
	[ch_id] [varchar](50) NOT NULL,
	[chart_type] [varchar](30) NOT NULL,
	[feedback_category] [int] NOT NULL,
	[flagged_root_cause_step] [varchar](200) NOT NULL,
	[flagged_root_cause_step_type] [varchar](200) NOT NULL,
	[flagged_root_cause_type] [varchar](200) NOT NULL,
	[flagged_root_cause] [varchar](200) NOT NULL,
	[actual_root_cause_step] [varchar](200) NOT NULL,
	[actual_root_cause_step_type] [varchar](200) NOT NULL,
	[actual_root_cause_type] [varchar](1000) NOT NULL,
	[actual_root_cause] [varchar](1000) NOT NULL,
	[report_link] [varchar](1000) NULL,
	[updated_datetime] [datetime] NULL,
PRIMARY KEY CLUSTERED
(
  [analysis_type] ASC,
	[fab] ASC,
	[session_id] ASC,
	[ch_id] ASC,
	[chart_type] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

