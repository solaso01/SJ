package com.sjzy.common.error;



public enum EmBusinessError implements CommonError {

	USERS_NOT_EXIT(10001,"这是啥");

	private EmBusinessError( int errCode,String errMsg){
		this.errCode = errCode;
		this.errMsg = errMsg;
		
	}
	
	private int errCode;
	
	private String errMsg;

	@Override
	public int getErrCode() {

		return this.errCode;
	}

	@Override
	public String getErrMsg() {

		return this.errMsg;
	}

	@Override
	public CommonError setErrMsg(String errMsg) {
		this.errMsg = errMsg;
		return this;
	}

}
