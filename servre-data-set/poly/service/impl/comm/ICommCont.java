package poly.service.impl.comm;

/**
 * 공통 사용 변수 정의
 */
public interface ICommCont {

	final public String ConstFtpUploadHomeDir = "/data/quiz01";

	final public String ConstHadoopQuizHomeDir = "/QUIZ";

	// 인기 퀴즈의 수 (기본 값 : 500)
	final public int ContQuizRank = 500;

	// 데이터분석 보관일수 (기본값 : 14)
	final public int ContSaveDayForWordAnalysis = 1;

	// 데이터분석 보관일수 (기본값 : 14)
	final public int ContSaveDay = 14;

	// 데이터분석 일수 (기본값 : 최근 7일)
	final public int ContReadCntDay = 7;

	// 데이터분석에 사용될 소수점 자릿수 (기본값 : 소수점 4자리)
	final public double ContDataSize = 1000000d;

	// 빅데이터처리완료상태코드
	public String ContBigDataAnalysisEndStatus = "E";
}
