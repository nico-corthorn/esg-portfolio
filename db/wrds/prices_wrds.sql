-- drop table prices_wrds

CREATE TABLE prices_wrds
(
	PERMNO varchar(20), -- int
	PrimaryExch varchar(1),
	SecurityNm text,
	Ticker varchar(20),
	PERMCO varchar(20), -- int
	IssuerNm text,
	YYYYMMDD varchar(8),
	DlyCalDt varchar(8),
	DlyDelFlg varchar(1),
	DlyPrc real,
	DlyPrcFlg varchar(3),
	DlyCap numeric(14,2),
	DlyCapFlg varchar(3),
	DlyPrevPrc numeric(14,2),
	DlyPrevPrcFlg varchar(3),
	DlyPrevDt varchar(8),
	DlyPrevCap numeric(14,2),
	DlyPrevCapFlg varchar(3),
	DlyRet real,
	DlyRetx real,
	DlyRetI real,
	DlyRetMissFlg varchar(3),
	DlyRetDurFlg varchar(3),
	DlyOrdDivAmt numeric(14,2),
	DlyNonOrdDivAmt numeric(14,2),
	DlyFacPrc real,
	DlyDistRetFlg varchar(3),
	DlyVol numeric(14,1),
	DlyClose numeric(14,2),
	DlyLow numeric(14,2),
	DlyHigh numeric(14,2),
	DlyBid numeric(14,2),
	DlyAsk numeric(14,2),
	DlyOpen numeric(14,2),
	DlyNumTrd numeric(14,2),
	DlyMMCnt varchar(20), -- int
	DlyPrcVol numeric(14,2),
	ShrStartDt varchar(8),
	ShrEndDt varchar(8),
	ShrOut varchar(20), -- int
	ShrSource varchar(5),
	ShrFacType varchar(3),
	ShrAdrFlg varchar(1),
	DisExDt varchar(8),
	DisSeqNbr real,
	DisOrdinaryFlg varchar(1),
	DisType varchar(20),
	DisFreqType varchar(1),
	DisPaymentType varchar(10),
	DisDetailType varchar(20),
	DisTaxType varchar(1),
	DisOrigCurType varchar(10),
	DisDivAmt real,
	DisFacPr real,
	DisFacShr real,
	DisDeclareDt varchar(8),
	DisRecordDt varchar(8),
	DisPayDt varchar(8),
	DisPERMNO varchar(20), -- int
	DisPERMCO varchar(20) -- int
)

CREATE INDEX IF NOT EXISTS prices_wrds_idx_date ON prices_wrds(dlycaldt);


