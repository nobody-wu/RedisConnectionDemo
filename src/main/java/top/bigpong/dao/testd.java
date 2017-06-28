package top.bigpong.dao;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

/**
 * Created by Cornelius on 2017/6/27.
 */
@Controller
@RequestMapping("/api")
public class testd {
	
	@ResponseBody
	@RequestMapping(value = "/getListInfo" , method = RequestMethod.POST)
	public String getListInfo(@RequestBody Map<String, Object> params) {
		return "ok";
	}

}
