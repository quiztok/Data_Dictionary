package poly.service.impl.data.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
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
import poly.service.impl.data.IPersonalDataService;
import poly.util.CmmUtil;
import poly.util.DateUtil;

@Service("PersonalDataService")
public class PersonalDataService extends MongoDBComon implements IPersonalDataService, ICommCont {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MongoTemplate mongodb;

	/**
	 * 학습된 퀴즈팩 정보가져오기
	 */
	private List<String> getQuizPackNN(String qp_id) throws Exception {

		log.info(this.getClass().getName() + ".getQuizPackNN Start!");

		// 조회할 컬렉션
		String colNm = "NLP_QUIZPACK_DICTIONARY";

		Document projection = new Document();

		projection.append("qp_id", "$qp_id");
		projection.append("nn", "$nn");
		projection.append("_id", 0);

		MongoCollection<Document> col = mongodb.getCollection(colNm);

		// 컬렉션 정보가져오기
		FindIterable<Document> rs = col.find(new Document("qp_id", qp_id)).projection(projection);

		// 조회되는 레코드가 1개이기 때문에 first 사용함
		Document doc = rs.first();

		if (doc == null) {
			doc = new Document();

		}

		List<String> nnList = doc.getList("nn", String.class, new LinkedList<String>());

		if (nnList == null) {

			nnList = new LinkedList<String>();

		}

		log.info(this.getClass().getName() + ".getQuizPackNN End!");

		return nnList;
	}

	/**
	 * 학습된 퀴즈로그 정보가져오기
	 */
	private List<String> getQuizLogNN(List<Document> pList) throws Exception {

		log.info(this.getClass().getName() + ".getQuizLogNN Start!");

		if (pList == null) {
			pList = new LinkedList<Document>();

		}

		// 조회할 컬렉션
		String colNm = "NLP_QUIZLOG_DICTIONARY";

		Document projection = new Document();

		projection.append("qp_id", "$qp_id");
		projection.append("nn", "$nn");
		projection.append("_id", 0);

		MongoCollection<Document> col = mongodb.getCollection(colNm);

		Iterator<Document> it = pList.iterator();

		List<String> nnList = new LinkedList<String>();

		while (it.hasNext()) {
			Document doc = (Document) it.next();

			if (doc == null) {
				doc = new Document();

			}

			String qp_id = CmmUtil.nvl(doc.getString("qp_id"));

			// 컬렉션 정보가져오기
			FindIterable<Document> rs = col.find(new Document("qp_id", qp_id)).projection(projection);

			// 조회되는 레코드가 1개이기 때문에 first 사용함
			Document fDoc = rs.first();

			if (fDoc == null) {
				fDoc = new Document();

			}

			nnList.addAll(fDoc.getList("nn", String.class, new LinkedList<String>()));

			rs = null;
			fDoc = null;
			doc = null;
		}

		log.info(this.getClass().getName() + ".getQuizLogNN End!");

		return nnList;
	}

	/**
	 * 퀴즈팩에 해당되는 퀴즈ID가져오기
	 */
	private List<String> getQuizId(String qp_id) throws Exception {

		log.info(this.getClass().getName() + ".getQuizId Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		// 조회할 컬렉션
		String colNm = "QUIZRANK_DATA_BN_" + ContAnaysisStdDay + "_STEP1";

		Document projection = new Document();

		projection.append("quiz_id", "$quiz_id");
		projection.append("_id", 0);

		MongoCollection<Document> col = mongodb.getCollection(colNm);

		// 컬렉션 정보가져오기
		FindIterable<Document> rs = col.find(new Document("qp_id", qp_id)).projection(projection);
		Iterator<Document> cursor = rs.iterator();

		List<Document> rList = IteratorUtils.toList(cursor);

		rs = null;
		cursor = null;

		log.info(this.getClass().getName() + ".getQuizId End!");

		return getQuizLogNN(rList);
	}

	/**
	 * 퀴즈풀이자 정보가져오기
	 */
	private int dataProcessStep1() throws Exception {
		log.info(this.getClass().getName() + ".dataProcessStep1 Start!");

		int res = 0;

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		// 조회할 컬렉션
		String colNm = "QUIZRANK_DATA_BN_" + ContAnaysisStdDay + "_STEP1";

		// 데이터 저장하기
		MongoCollection<Document> col = mongodb.getCollection(colNm);

		// 실행 쿼리
		List<? extends Bson> pipeline = Arrays.asList(new Document().append("$group",
				new Document().append("_id", new Document().append("user_id", "$user_id").append("qp_id", "$qp_id"))
						.append("AVG(answerTrue)", new Document().append("$avg", "$answerTrue"))
						.append("AVG(answerFalse)", new Document().append("$avg", "$answerFalse"))),
				new Document().append("$project",
						new Document().append("qp_id", "$_id.qp_id").append("user_id", "$_id.user_id")
								.append("answerTrue", "$AVG(answerTrue)").append("answerFalse", "$AVG(answerFalse)")
								.append("_id", 0)));

		AggregateIterable<Document> rs = col.aggregate(pipeline).allowDiskUse(true);
		Iterator<Document> cursor = rs.iterator();

		// 저장할 데이터
		List<Document> rList = IteratorUtils.toList(cursor);

		if (rList == null) {
			rList = new ArrayList<Document>();
		}

		rs = null;
		cursor = null;
		col = null;

		// 생성할 컬렉션
		colNm = "PERSONAL_ANALYSIS_DATA_" + ContAnaysisStdDay + "_STEP1";

		String[] colIndex = { "user_id", "qp_id" };

		super.DeleteCreateCollectionUniqueIndex(colNm, colIndex);

		// 데이터 저장하기
		col = mongodb.getCollection(colNm);
		col.insertMany(rList);
		col = null;

		rList = null;

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
		String colNm = "PERSONAL_ANALYSIS_DATA_" + ContAnaysisStdDay + "_STEP1";

		Document projection = new Document();

		projection.append("user_id", "$user_id");
		projection.append("qp_id", "$qp_id");
		projection.append("answerTrue", "$answerTrue");
		projection.append("answerFalse", "$answerFalse");
		projection.append("_id", 0);

		MongoCollection<Document> col = mongodb.getCollection(colNm);

		// 컬렉션 정보가져오기
		FindIterable<Document> rs = col.find(new Document()).projection(projection);
		Iterator<Document> cursor = rs.iterator();

		while (cursor.hasNext()) {

			Document doc = cursor.next();

			if (doc == null) {
				doc = new Document();

			}

			String user_id = CmmUtil.nvl(doc.getString("user_id"));
			String qp_id = CmmUtil.nvl(doc.getString("qp_id"));
			String answerTrue = CmmUtil.nvl(doc.getString("answerTrue"));
			String answerFalse = CmmUtil.nvl(doc.getString("answerFalse"));

			log.info("qp_id : " + qp_id);

			// 퀴즈팩NN 가져오기
			List<String> qpNN = this.getQuizPackNN(qp_id);

			// 퀴즈로그NN 가져오기
			List<String> quizNN = this.getQuizId(qp_id);

			// 퀴즈팩과 퀴즈로그 nn 합치기

			doc = null;

		}

		rs = null;
		cursor = null;
		col = null;

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
	public int doDataProcess() throws Exception {
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
