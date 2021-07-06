package poly.service.impl.comm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

public abstract class MongoDBComon {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MongoTemplate mongodb;

	/**
	 * 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스  컬럼
	 * 
	 */
	public void createCollection(String colNm, String colume) throws Exception {
		createCollection(colNm, colume, true);

	}

	/**
	 * 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스  컬럼
	 * 
	 */
	public void createCollection(String colNm, String[] colume) throws Exception {
		createCollection(colNm, colume, true);

	}

	/**
	 * 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void createCollection(String colNm, String colume, boolean order) throws Exception {

		log.info("createCollection Start! - colNm : " + colNm);

		if (mongodb.collectionExists(colNm)) {

			log.info("collectionExists colNm : " + colNm);

		} else {
			// 컬렉션 생성
			if (order) {
				mongodb.createCollection(colNm).createIndex(Indexes.ascending(colume));

			} else {
				mongodb.createCollection(colNm).createIndex(Indexes.descending(colume));

			}

		}

		log.info("createCollection End! - colNm : " + colNm);
	}

	/**
	 * 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void createCollection(String colNm, String[] colume, boolean order) throws Exception {

		log.info("createCollection Start! - colNm : " + colNm);

		if (mongodb.collectionExists(colNm)) {

			log.info("collectionExists colNm : " + colNm);

		} else {
			// 컬렉션 생성
			if (order) {
				mongodb.createCollection(colNm).createIndex(Indexes.ascending(colume));

			} else {
				mongodb.createCollection(colNm).createIndex(Indexes.descending(colume));

			}

		}

		log.info("createCollection End! - colNm : " + colNm);
	}

	/**
	 * 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스  컬럼
	 * 
	 */
	public void createCollectionUniqueIndex(String colNm, String[] colume) throws Exception {
		createCollectionUniqueIndex(colNm, colume, true);

	}

	/**
	 * 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스  컬럼
	 * 
	 */
	public void createCollectionUniqueIndex(String colNm, String colume) throws Exception {
		createCollectionUniqueIndex(colNm, colume, true);

	}

	/**
	 * 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void createCollectionUniqueIndex(String colNm, String colume, boolean order) throws Exception {

		log.info("createCollectionUniqueIndex Start! - colNm : " + colNm);

		if (mongodb.collectionExists(colNm)) {

			log.info("collectionExists colNm : " + colNm);

		} else {
			IndexOptions indexOptions = new IndexOptions().unique(true);

			// 컬렉션 생성
			if (order) {
				mongodb.createCollection(colNm).createIndex(Indexes.ascending(colume), indexOptions);

			} else {
				mongodb.createCollection(colNm).createIndex(Indexes.descending(colume), indexOptions);

			}

		}

		log.info("createCollectionUniqueIndex End! - colNm : " + colNm);
	}

	/**
	 * 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void createCollectionUniqueIndex(String colNm, String[] colume, boolean order) throws Exception {

		log.info("createCollectionUniqueIndex Start! - colNm : " + colNm);

		if (mongodb.collectionExists(colNm)) {

			log.info("collectionExists colNm : " + colNm);

		} else {
			IndexOptions indexOptions = new IndexOptions().unique(true);

			// 컬렉션 생성
			if (order) {
				mongodb.createCollection(colNm).createIndex(Indexes.ascending(colume), indexOptions);

			} else {
				mongodb.createCollection(colNm).createIndex(Indexes.descending(colume), indexOptions);

			}

		}

		log.info("createCollectionUniqueIndex End! - colNm : " + colNm);
	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스  컬럼
	 * 
	 */
	public void DeleteCreateCollection(String colNm, String colume) throws Exception {
		DeleteCreateCollection(colNm, colume, true);

	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스  컬럼
	 * 
	 */
	public void DeleteCreateCollection(String colNm, String[] colume) throws Exception {
		DeleteCreateCollection(colNm, colume, true);

	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void DeleteCreateCollection(String colNm, String colume, boolean order) throws Exception {

		log.info("DeleteCreateCollection Start! - colNm : " + colNm);

		// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
		if (mongodb.collectionExists(colNm)) {
			mongodb.dropCollection(colNm);

		}

		// 컬렉션 생성
		if (order) {
			mongodb.createCollection(colNm).createIndex(Indexes.ascending(colume));

		} else {
			mongodb.createCollection(colNm).createIndex(Indexes.descending(colume));

		}

		log.info("DeleteCreateCollection End! - colNm : " + colNm);

	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void DeleteCreateCollection(String colNm, String[] colume, boolean order) throws Exception {

		log.info("DeleteCreateCollection Start! - colNm : " + colNm);

		// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
		if (mongodb.collectionExists(colNm)) {
			mongodb.dropCollection(colNm);

		}

		// 컬렉션 생성
		if (order) {
			mongodb.createCollection(colNm).createIndex(Indexes.ascending(colume));

		} else {
			mongodb.createCollection(colNm).createIndex(Indexes.descending(colume));

		}

		log.info("DeleteCreateCollection End! - colNm : " + colNm);

	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 샤딩 가능한 컬렉션으로 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void DeleteCreateCollection(String colNm, Map<String, Boolean> colume) throws Exception {

		log.info("DeleteCreateCollection Start! - colNm : " + colNm);

		// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
		if (mongodb.collectionExists(colNm)) {
			mongodb.dropCollection(colNm);

			log.info("Delete Collection : " + colNm);

		}

		Set<String> rIndex = colume.keySet();

		if (rIndex == null) {
			rIndex = new HashSet<String>();

		}

		Iterator<String> it = rIndex.iterator();

		while (it.hasNext()) {
			String key = it.next();

			log.info("Create colNm : " + colNm + " / createIndex : " + key);

			// 유니크한 인덱스는 인덱스 컬럼명 앞에 소문자 unique_ 붙임
			if (key.contains("unique_")) {
				IndexOptions indexOptions = new IndexOptions().unique(true);

				// 인덱스 컬럼명에서 unique_ 단어 제거하기
				String col = key.replaceAll("unique_", "");

				if (colume.get(key)) {

					if (mongodb.collectionExists(colNm)) {
						mongodb.getCollection(colNm).createIndex(Indexes.ascending(col), indexOptions);

					} else {
						mongodb.createCollection(colNm).createIndex(Indexes.ascending(col), indexOptions);

					}

				} else {
					if (mongodb.collectionExists(colNm)) {
						mongodb.getCollection(colNm).createIndex(Indexes.ascending(col), indexOptions);

					} else {
						mongodb.createCollection(colNm).createIndex(Indexes.ascending(col), indexOptions);

					}

				}

				indexOptions = null;

			} else { // 유니크 키 없는 컬렉션이라면..
				if (colume.get(key)) {
					if (mongodb.collectionExists(colNm)) {
						mongodb.getCollection(colNm).createIndex(Indexes.ascending(key));

					} else {
						mongodb.createCollection(colNm).createIndex(Indexes.ascending(key));

					}

				} else {
					if (mongodb.collectionExists(colNm)) {
						mongodb.getCollection(colNm).createIndex(Indexes.ascending(key));

					} else {
						mongodb.createCollection(colNm).createIndex(Indexes.ascending(key));

					}

				}

			}

		}

		it = null;
		rIndex = null;

		log.info("DeleteCreateCollection End! - colNm : " + colNm);

	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 유니크한 인덱스 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void DeleteCreateCollectionUniqueIndex(String colNm, String colume) throws Exception {
		DeleteCreateCollectionUniqueIndex(colNm, colume, true);

	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 유니크한 인덱스 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void DeleteCreateCollectionUniqueIndex(String colNm, String[] colume) throws Exception {
		DeleteCreateCollectionUniqueIndex(colNm, colume, true);

	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 유니크한 인덱스 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void DeleteCreateCollectionUniqueIndex(String colNm, String colume, boolean order) throws Exception {

		log.info("DeleteCreateCollectionUniqueIndex Start! - colNm : " + colNm);

		// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
		if (mongodb.collectionExists(colNm)) {
			mongodb.dropCollection(colNm);

		}

		IndexOptions indexOptions = new IndexOptions().unique(true);

		// 컬렉션 생성
		if (order) {
			mongodb.createCollection(colNm).createIndex(Indexes.ascending(colume), indexOptions);

		} else {
			mongodb.createCollection(colNm).createIndex(Indexes.descending(colume), indexOptions);

		}

		log.info("DeleteCreateCollectionUniqueIndex End! - colNm : " + colNm);

	}

	/**
	 * 기존 이름의 컬렉션을 삭제하고, 유니크한 인덱스 생성하기
	 * 
	 * @param 컬렉션명
	 * @param 인덱스          컬럼
	 * @param 인덱스정렬방식(true :오름차순 / false : 내림차순)
	 * 
	 */
	public void DeleteCreateCollectionUniqueIndex(String colNm, String[] colume, boolean order) throws Exception {

		log.info("DeleteCreateCollectionUniqueIndex Start! - colNm : " + colNm);

		// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
		if (mongodb.collectionExists(colNm)) {
			mongodb.dropCollection(colNm);

		}

		IndexOptions indexOptions = new IndexOptions().unique(true);

		// 컬렉션 생성
		if (order) {
			mongodb.createCollection(colNm).createIndex(Indexes.ascending(colume), indexOptions);

		} else {
			mongodb.createCollection(colNm).createIndex(Indexes.descending(colume), indexOptions);

		}

		log.info("DeleteCreateCollectionUniqueIndex End! - colNm : " + colNm);

	}

	/**
	 * 분할 데이터 저장 기본 분할 단위 : 2000개
	 * 
	 * @param 컬렉션명
	 * @param 저장할  리스트
	 * 
	 */
	public void insertMany(String pColNm, List<Document> pList) throws Exception {
		this.insertMany(pColNm, pList, 2000);

	}

	/**
	 * 분할 데이터 저장
	 * 
	 * @param 컬렉션명
	 * @param 저장할  리스트
	 * @param 한번에  저장할 최대 리스트 수
	 * 
	 */
	public void insertMany(String pColNm, List<Document> pList, int pBlock) throws Exception {

		log.info("insertMany Start! - colNm : " + pColNm);

		// 분석 데이터 크기에 따라 분할 저장
		int sListCnt = pList.size();

		for (int i = 0; i < sListCnt; i += pBlock) {

			// log.info("[" + pColNm + "] " + i + " Block");
			MongoCollection<Document> col = mongodb.getCollection(pColNm);
			col.insertMany(new ArrayList<>(pList.subList(i, Math.min(i + pBlock, sListCnt))));
			col = null;

		}

		log.info("insertMany End! - colNm : " + pColNm);

	}

	/**
	 * 개인화 분석 저장 컬렉션명 가져오기
	 * 
	 * @param 컬렉션명
	 * @param 저장할  리스트
	 * @param 한번에  저장할 최대 리스트 수
	 * 
	 */
	public String getUserAnaysisColNm(String user_id) throws Exception {

		log.info("getUserAnaysisColNm Start!");

		char id = user_id.charAt(0);

		// 회원아이디 첫글자를 기반으로 그룹 나누기
		int grp = id % 16;

		String colNm = "USER_ANALYSIS_TYPE" + grp;

		log.info("getUserAnaysisColNm End!");

		return colNm;

	}

	/**
	 * 이전 빅데이터분석 완료여부(시작하기전 이전 작업 완료되었는지 체크함)
	 * 
	 * @param 분석날짜
	 * @param 분석단계
	 * 
	 */
	public boolean doAnalysisStart(String today, String step) throws Exception {

		log.info("doAnalysisStart Start! today : " + today + " / step : " + step);

		boolean res = false;

		Document query = new Document();

		query.append("std_day", today);
		query.append("step", step);
		query.append("status", ICommCont.ContBigDataAnalysisEndStatus); // 작업완료코드

		Document projection = new Document();

		projection.append("std_day", "$std_day");
		projection.append("step", "$step");
		projection.append("status", "$status");
		projection.append("_id", 0);

		MongoCollection<Document> col = mongodb.getCollection("BIGDATA_ANALYSIS_INTERFACE");

		// 컬렉션 정보가져오기
		FindIterable<Document> rs = col.find(query).projection(projection);
		Iterator<Document> cursor = rs.iterator();

		if (cursor.hasNext()) {
			res = true;

		}

		rs = null;
		cursor = null;
		col = null;

		projection = null;
		query = null;

		log.info("doAnalysisStart End! today : " + today + " / step : " + step);

		return res;

	}

	/**
	 * 빅데이터 분석 상태값 추가하기
	 * 
	 * @param 분석날짜
	 * @param 분석단계
	 * @param 분석상태
	 * 
	 */
	public void addAnalysisStatus(String today, String step, String status) throws Exception {

		log.info("addAnalysisStatus Start! today : " + today + " / step : " + step);

		String colNm = "BIGDATA_ANALYSIS_INTERFACE";

		Document query = new Document();

		query.append("std_day", today);
		query.append("step", step);
		query.append("status", ICommCont.ContBigDataAnalysisEndStatus); // 작업완료코드

		Document projection = new Document();

		projection.append("std_day", "$std_day");
		projection.append("step", "$step");
		projection.append("status", "$status");
		projection.append("_id", 0);

		MongoCollection<Document> col = mongodb.getCollection(colNm);

		// 컬렉션 정보가져오기
		FindIterable<Document> rs = col.find(query).projection(projection);
		Iterator<Document> cursor = rs.iterator();

		// 기존 등록된 데이터 삭제
		if (cursor.hasNext()) {
			col.deleteOne(cursor.next());
		}

		cursor = null;
		rs = null;
		col = null;

		projection = null;
		query = null;

		Document doc = new Document();

		doc.append("std_day", today);
		doc.append("step", step);
		doc.append("status", status);

		col = mongodb.getCollection(colNm);
		col.insertOne(doc);
		col = null;

		doc = null;

		log.info("addAnalysisStatus End! today : " + today + " / step : " + step);

	}

	/**
	 * 빅데이터 분석 종료 상태값 추가하기
	 * 
	 * @param 분석날짜
	 * @param 분석단계
	 * 
	 */
	public void addAnalysisStatusEnd(String today, String step) throws Exception {
		addAnalysisStatus(today, step, ICommCont.ContBigDataAnalysisEndStatus);

	}
}
