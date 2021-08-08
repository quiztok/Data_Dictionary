package poly.service.impl.data.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import kr.co.shineware.nlp.komoran.constant.DEFAULT_MODEL;
import kr.co.shineware.nlp.komoran.core.Komoran;
import kr.co.shineware.nlp.komoran.model.KomoranResult;
import poly.service.impl.comm.ICommCont;
import poly.service.impl.comm.IStopWord;
import poly.service.impl.comm.MongoDBComon;
import poly.service.impl.data.IQuizLogPersonalService;
import poly.util.CmmUtil;

@Service("QuizLogPersonalService")
public class QuizLogPersonalService extends MongoDBComon implements IQuizLogPersonalService, ICommCont {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MongoTemplate mongodb;

	Komoran nlp = null;

	// 생성자 사용
	private QuizLogPersonalService() {

		log.info(this.getClass().getName() + ".QuizLogPersonalService creator Start!");

		// NLP 분석 객체 메모리 로딩
		this.nlp = new Komoran(DEFAULT_MODEL.LIGHT);

		// NLP 사용자 사전 로딩
//		this.nlp.setUserDic(IQuizPackDicService.userDic);

		log.info(this.getClass().getName() + ".QuizLogPersonalService creator End!");
	}

	/**
	 * NLP 분석 실행
	 * 
	 * @param 분석 문구
	 */
	private Set<String> doNlp(String text) {

		log.info(this.getClass().getName() + ".doNlp Start!");

		/*
		 * #############################################################################
		 * 형태소 분석 시작
		 * #############################################################################
		 */

		// 형태소 분석 시작
		KomoranResult analyzeResultList = this.nlp.analyze(text);

		List<String> rList = analyzeResultList.getNouns();

		if (rList == null) {
			rList = new ArrayList<String>();
		}

		Iterator<String> it = rList.iterator();

		// 데이터 중복 제거를 위해 Set 구조 사용
		Set<String> rSet = new HashSet<String>();

		// 단어 분석결과가 한글자 이상 데이터만 저장하기
		while (it.hasNext()) {
			String nn = CmmUtil.nvl(it.next());

			if (nn.length() > 1) {
				rSet.add(nn);

			}

		}
		it = null;
		rList = null;

		/*
		 * #############################################################################
		 * 형태소 분석 끝
		 * #############################################################################
		 */
		log.info(this.getClass().getName() + ".doNlp End!");

		return doSynonymDictionary(rSet);
	}

	/**
	 * 유사서 사전을 통한 데이터 정제
	 * 
	 * @param 형태소 분석 결과
	 * @return 정제 결과
	 */
	private Set<String> doSynonymDictionary(Set<String> pSet) {

		log.info(this.getClass().getName() + ".doSynonymDictionary Start!");

		String colNm = "NLP_SYNONYM_DICTIONARY";

		Iterator<String> it = pSet.iterator();

		Set<String> sSet = new TreeSet<String>();

		while (it.hasNext()) {
			String nn = CmmUtil.nvl(it.next()); // 분석된 단어

			if (!IStopWord.stopWord.contains(nn)) {
				log.info("nn : " + nn);

				// 컬렉션 정보가져오기
				MongoCollection<Document> col = mongodb.getCollection(colNm);

				Document projection = new Document();
				projection.append("tag", "$tag");
				projection.append("_id", 0);

				// 컬렉션 정보가져오기
				FindIterable<Document> rs = col.find(new Document("keywords", nn)).projection(projection);
				Iterator<Document> cursor = rs.iterator();

				if (cursor.hasNext()) {

					Document doc = cursor.next();
					nn = CmmUtil.nvl(doc.getString("tag"));

				}
				sSet.add(nn);
			}

		}

		log.info(this.getClass().getName() + ".doSynonymDictionary End!");

		return sSet;
	}

	private int dataProcessStep1() throws Exception {
		log.info(this.getClass().getName() + ".dataProcessStep1 Start!");

		int res = 0;

		String colNm = "AAA_QUIZ_RESULT";

		MongoCollection<Document> col = mongodb.getCollection(colNm);

		Document projection = new Document();
		projection.append("quizId", "$quizId");
		projection.append("title", "$title");
		projection.append("examples", "$examples.body");
		projection.append("explanation", "$explanation");
		projection.append("_id", 0);

		FindIterable<Document> rs = col.find(new Document()).projection(projection);
		Iterator<Document> cursor = rs.iterator();

		List<Document> sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			Document doc = cursor.next();
			String quizId = CmmUtil.nvl(doc.getString("quizId"));
			String title = CmmUtil.nvl(doc.getString("title"));
			String explanation = CmmUtil.nvl(doc.getString("explanation"));
			List<Document> eList = doc.getList("examples", Document.class);

			if (eList == null) {
				eList = new ArrayList<Document>();

			}

			String ex = "";

			for (Document eDoc : eList) {
				ex += (" ," + CmmUtil.nvl(eDoc.getString("body")));

			}

			int exLenth = ex.length();
			if (exLenth > 0) {
				ex = ex.substring(1, exLenth);

			}
			
			log.info("quizId : "+ quizId);
			log.info("title : "+ title);
			log.info("explanation : "+ explanation);
			log.info("ex : "+ ex);

			// 분석 결과
			Set<String> rSet = new HashSet<String>();

			rSet.addAll(doNlp(title));
			rSet.addAll(doNlp(explanation));
			rSet.addAll(doNlp(ex));

			doc.append("nn", rSet);

			sList.add(doc);

			doc = null;

		}

		colNm = "AAA_QUIZ_RESULT_FIN";

		col = mongodb.getCollection(colNm);
		col.insertMany(sList);
		col = null;

		sList = null;

		res = 1;

		log.info(this.getClass().getName() + ".dataProcessStep1 End!");

		return res;
	}

	@Override
	public int doProcess() throws Exception {
		int res = 0;

		// NLP 처리 시작 시간
		long startTime = System.currentTimeMillis();

		// 퀴즈팩 데이터 합치기
		if (this.dataProcessStep1() != 1) {
			return 0;
		}

		// NLP 처리 종료 시간
		long endTime = System.currentTimeMillis();

		log.info("NLP Personal Execute Time(s) : " + (endTime - startTime) / 1000.0 + " sec");

		res = 1;

		return res;
	}

}
