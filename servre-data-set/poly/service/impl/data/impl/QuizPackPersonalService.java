package poly.service.impl.data.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import poly.service.impl.comm.ICommCont;
import poly.service.impl.comm.MongoDBComon;
import poly.service.impl.data.IQuizPackPersonalService;
import poly.util.CmmUtil;
import poly.util.DateUtil;

@Service("QuizPackPersonalService")
public class QuizPackPersonalService extends MongoDBComon implements IQuizPackPersonalService, ICommCont {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MongoTemplate mongodb;

	/**
	 * 최근 14일동안의 퀴즈팩 데이터 가져오기 1단계
	 */
	private int dataProcessStep1() throws Exception {
		log.info(this.getClass().getName() + ".dataProcessStep1 Start!");

		int res = 0;

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		Iterator<Document> cursor = null;
		FindIterable<Document> rs = null;

		// 생성할 컬렉션
		String colNm = "NLP_QUIZPACK_PERSONAL_" + ContAnaysisStdDay + "_STEP1";

		// 컬렉션 인덱스
		String[] colIdx = { "qp_id", "user_id" };

		// 최근 14일동안의 퀴즈팩 정보 저장할 컬렉션 생성하기
		super.DeleteCreateCollection(colNm, colIdx);

		for (int i = 0; i < ICommCont.ContSaveDayForWordAnalysis; i++) {

			String stdDay = DateUtil.getDateTimeAdd(-i); // 분석 일자

//			QUIZRANK_DATA_BN_20200929_STEP2
			String dColNm = "QUIZRANK_DATA_BN_" + stdDay + "_STEP2";

			log.info("Get Data Collection : " + dColNm);

			if (mongodb.collectionExists(dColNm)) {

				log.info("Exists Collection : " + dColNm);

				// 컬렉션 정보가져오기
				MongoCollection<Document> rCol = mongodb.getCollection(dColNm);

				Document query = new Document();
				query.append("answerTrue", new Document().append("$gt", 0L));

				// 출력
				Document projection = new Document();
				projection.append("qp_id", "$qp_id");
				projection.append("user_id", "$user_id");
				projection.append("answerTrue", "$answerTrue");
				projection.append("answerTrueRate", "$answerTrueRate");
				projection.append("_id", 0);

				rs = rCol.find(query).projection(projection);
				cursor = rs.iterator(); // 컬렉션 데이터 가져오기

				projection = null;
				query = null;

				// 컬렉션 데이터를 List 형태로 변환
				List<Document> rList = IteratorUtils.toList(cursor);

				if (rList == null) {
					rList = new ArrayList<Document>();
				}

				// 데이터 저장하기(메모리 부족 현상이 발생할 수 있기 때문에 반드시 컬렉션별 나눠서 저장한다.)
				MongoCollection<Document> col = mongodb.getCollection(colNm);
				col.insertMany(rList);
				col = null;

				rCol = null;
				rList = null;
				cursor = null;
				rs = null;

			} else {
				log.info("Do Not Exists Collection : " + dColNm);

			}

		}

		res = 1;

		log.info(this.getClass().getName() + ".dataProcessStep1 End!");

		return res;
	}

	/**
	 * 퀴즈팩, 유저별 중복 데이터 제거
	 */
	private int doProcessStep2() throws Exception {
		log.info(this.getClass().getName() + ".doProcessStep2 Start!");

		int res = 0;

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		// 조회할 컬렉션
		String rColNm = "NLP_QUIZPACK_PERSONAL_" + ContAnaysisStdDay + "_STEP1";

		// 데이터 저장하기
		MongoCollection<Document> col = mongodb.getCollection(rColNm);

		// 실행 쿼리
		List<? extends Bson> pipeline = Arrays.asList(new Document().append("$group",
				new Document().append("_id", new Document().append("user_id", "$user_id").append("qp_id", "$qp_id"))
						.append("AVG(answerTrue)", new Document().append("$avg", "$answerTrue"))
						.append("AVG(answerTrueRate)", new Document().append("$avg", "$answerTrueRate"))
						.append("COUNT(qp_id)", new Document().append("$sum", 1))),
				new Document().append("$project",
						new Document().append("qp_id", "$_id.qp_id").append("user_id", "$_id.user_id")
								.append("answerTrue", "$AVG(answerTrue)")
								.append("answerTrueRate", "$AVG(answerTrueRate)").append("qp_cnt", "$COUNT(qp_id)")
								.append("_id", 0)),
				new Document().append("$match", new Document().append("answerTrue", new Document().append("$gt", 0L))));

		AggregateIterable<Document> rs = col.aggregate(pipeline).allowDiskUse(true);
		Iterator<Document> cursor = rs.iterator();

		col = null;

		// 저장할 데이터
		List<Document> rList = IteratorUtils.toList(cursor);

		if (rList == null) {
			rList = new ArrayList<Document>();
		}

		// 생성할 컬렉션
		String nColNm = "NLP_QUIZPACK_PERSONAL_" + ContAnaysisStdDay + "_STEP2";

		String[] colIndex = { "qp_id", "user_id" };

		super.DeleteCreateCollectionUniqueIndex(nColNm, colIndex);

		// 데이터 저장하기
		col = mongodb.getCollection(nColNm);
		col.insertMany(rList);
		col = null;

		rList = null;

		res = 1;

		log.info(this.getClass().getName() + ".doProcessStep2 End!");

		return res;
	}

	/**
	 * 데이터 사전과 사용자별 퀴즈팩 데이터 매칭
	 */
	private int dataProcessStep3() throws Exception {
		log.info(this.getClass().getName() + ".dataProcessStep3 Start!");

		int res = 0;

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		// 기준 컬렉션
		String stdColNm = "NLP_QUIZPACK_PERSONAL_" + ContAnaysisStdDay + "_STEP2";

		// 기준 컬렉션
		String joinColNm = "NLP_QUIZPACK_DICTIONARY";

		List<? extends Bson> pipeline = Arrays
				.asList(new Document().append("$project", new Document().append("_id", 0).append(stdColNm, "$$ROOT")),
						new Document().append("$lookup",
								new Document().append("localField", stdColNm + ".qp_id").append("from", joinColNm)
										.append("foreignField", "qp_id").append("as", joinColNm)),
						new Document()
								.append("$unwind",
										new Document()
												.append("path",
														"$" + joinColNm)
												.append("preserveNullAndEmptyArrays", false)),
						new Document().append("$project",
								new Document().append("user_id", "$" + stdColNm + ".user_id")
										.append("qp_id", "$" + stdColNm + ".qp_id")
										.append("answerTrue", "$" + stdColNm + ".answerTrue")
										.append("answerTrueRate", "$" + stdColNm + ".answerTrueRate")
										.append("qp_cnt", "$" + stdColNm + ".qp_cnt")
										.append("subject", "$" + joinColNm + ".subject")
										.append("tags", "$" + joinColNm + ".tags").append("nn", "$" + joinColNm + ".nn")
										.append("_id", 0)));

		MongoCollection<Document> col = mongodb.getCollection(stdColNm);

		AggregateIterable<Document> rs = col.aggregate(pipeline).allowDiskUse(true);
		Iterator<Document> cursor = rs.iterator();

		List<Document> sList = new ArrayList<Document>();

		while (cursor.hasNext()) {
			Document doc = cursor.next();

			// 퀴즈팩 이름
			String subject = CmmUtil.nvl(doc.getString("subject"));

			// 오늘의 퀴즈는 분석 제외
			if (subject.indexOf("오늘의 퀴즈") > 0) {
				log.info("subjec333t : " + subject);

			} else {
				sList.add(doc);

			}
		}

		rs = null;
		cursor = null;
		col = null;

		// 생성할 컬렉션
		String nColNm = "NLP_QUIZPACK_PERSONAL_" + ContAnaysisStdDay;

		String[] colIndex = { "user_id", "qp_id" };

		super.DeleteCreateCollectionUniqueIndex(nColNm, colIndex);

		// 데이터 저장하기
		col = mongodb.getCollection(nColNm);
		col.insertMany(sList);
		col = null;

		res = 1;

		log.info(this.getClass().getName() + ".dataProcessStep3 End!");

		return res;
	}

	/**
	 * 사용 완료된 임시 컬렉션 삭제
	 */
	private int doClean() throws Exception {
		log.info(this.getClass().getName() + ".doClean Start!");

		int res = 0;

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");
		String preStdDay = DateUtil.getDateTimeAdd(-1); // 분석 일자

		// 1단계 삭제
		String colNm = "NLP_QUIZPACK_PERSONAL_" + ContAnaysisStdDay + "_STEP1";

		if (mongodb.collectionExists(colNm)) {
			mongodb.dropCollection(colNm);
			log.info("Drop Collection : " + colNm);

		}

		// 2단계 삭제
		colNm = "NLP_QUIZPACK_PERSONAL_" + ContAnaysisStdDay + "_STEP2";

		if (mongodb.collectionExists(colNm)) {
			mongodb.dropCollection(colNm);
			log.info("Drop Collection : " + colNm);

		}

		// 이전 분석 최종 결과 삭제
		colNm = "NLP_QUIZPACK_PERSONAL_" + preStdDay;

		if (mongodb.collectionExists(colNm)) {
			mongodb.dropCollection(colNm);
			log.info("Drop Collection : " + colNm);

		}

		res = 1;

		log.info(this.getClass().getName() + ".doClean End!");

		return res;
	}

	@Override
	public int doProcess() throws Exception {
		int res = 0;

		long startTime = System.currentTimeMillis();

		// 퀴즈 푼 퀴즈팩 데이터 합치기
		if (this.dataProcessStep1() != 1) {
			return 0;
		}

		// 중복 제거
		if (this.doProcessStep2() != 1) {
			return 0;
		}

		// 데이터 사전과 사용자별 퀴즈팩 데이터 매칭
		if (this.dataProcessStep3() != 1) {
			return 0;
		}

		// 임시 컬렉션 삭제
		if (this.doClean() != 1) {
			return 0;
		}

		long endTime = System.currentTimeMillis();

		log.info("Personal Data Execute Time(s) : " + (endTime - startTime) / 1000.0 + " sec");

		res = 1;

		return res;
	}

}
