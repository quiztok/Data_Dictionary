package poly.util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;

public class CmmUtil {
	public static String nvl(String str, String chg_str) {
		String res;

		if (str == null) {
			res = chg_str;
		} else if (str.equals("")) {
			res = chg_str;
		} else {
			res = str;
		}
		return res;
	}

	public static String nvl(String str) {
		return nvl(str, "");
	}

	/**
	 * 한국어, 영어, 숫자 및 한칸을 제외하고 다 제거하기
	 * 
	 * @param 변경할 문자열
	 * @return 변경된 문자열
	 */
	public static String replaceText(String str) {

		String replaceStr = str.replaceAll("[^가-힣a-zA-Z0-9/s]", " ");

		return replaceStr.trim();
	}

	public static String checked(String str, String com_str) {
		if (str.equals(com_str)) {
			return " checked";
		} else {
			return "";
		}
	}

	public static String checked(String[] str, String com_str) {
		for (int i = 0; i < str.length; i++) {
			if (str[i].equals(com_str))
				return " checked";
		}
		return "";
	}

	public static String select(String str, String com_str) {
		if (str.equals(com_str)) {
			return " selected";
		} else {
			return "";
		}
	}

	public static String getFileNm(String fileFullPath) {
		int pos = fileFullPath.lastIndexOf("/"); // 리눅스 운영체제용

		if (pos < 1) { // 윈도우 운영체제용
			pos = fileFullPath.lastIndexOf("\\");
		}

		String fileName = fileFullPath.substring(pos + 1, fileFullPath.length());

		return nvl(fileName);

	}

	/**
	 * 숫자 타입으로 변환(Long)
	 * 
	 */
	public static long nvl(Object obj, long num) {
		long res = 0;

		if (obj == null) {
			res = num;

		} else {
			res = (long) obj;

		}

		return res;
	}

	/**
	 * 숫자 타입으로 변환(Long)
	 * 
	 */
	public static long nvl(Object obj) {
		return nvl(obj, 0);
	}

	/**
	 * 숫자 타입으로 변환(Long)
	 * 
	 */
	public static double nvl(Object obj, double num) {
		double res = 0;

		if (obj == null) {
			res = num;

		} else {
			res = (double) obj;

		}

		return res;
	}

	/**
	 * 첫글자가 특수문자인 체크하기
	 */
	public static int startCharCheck(String str) {

		int res = 0;

		Pattern p = Pattern.compile("^[!@#$%^&*]");
		Matcher m = p.matcher(str);

		if (m.find()) {
			res = 1;
		}

		return res;
	}

	/**
	 * 첫글자가 특수문자인 체크하기
	 */
	public static int endCharCheck(String str) {

		int res = 0;

		Pattern p = Pattern.compile("[!@#$%^&*?]$");
		Matcher m = p.matcher(str);

		if (m.find()) {
			res = 1;
		}

		return res;
	}

	/**
	 * 맵 데이터구조의 값에 따른 정렬(내리차순)
	 * 
	 */
	public static Document desc(Map<String, Long> map) {
		List<Map.Entry<String, Long>> entries = new LinkedList<>(map.entrySet());
		Collections.sort(entries, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));

		Document res = new Document();

		for (Map.Entry<String, Long> entry : entries) {
			res.append(entry.getKey(), entry.getValue());

		}

		return res;
	}

}
