package poly.service.impl;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import poly.service.IBigDataService;
import poly.service.impl.comm.ICommCont;
import poly.service.impl.data.IPersonalDataService;

@Service("BigDataService")
public class BigDataService implements IBigDataService, ICommCont {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	// 개인화 분석을 위한 데이터셋 구성
	@Resource(name = "PersonalDataService")
	private IPersonalDataService personalDataService;

	@Scheduled(cron = "0 0 8 * * *")
	@Override
	public void doDataAnalysis() throws Exception {

		log.info(this.getClass().getName() + ".doDataAnalysis Start!!");

		// 개인화 분석을 위한 데이터셋 구성
		if (personalDataService.doDataProcess() != 1) {
			log.info(this.getClass().getName() + "personalDataService Fail !!");
			return;

		}

		return;
	}

}
