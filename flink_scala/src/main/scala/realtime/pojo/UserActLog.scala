package realtime.pojo

import java.sql.Date

/**
  * @Author ：zhd
  * @Date: 2022/1/13 19:11
  * @Dscription:
  */
case class UserActLog
(
  tenant_id:String,
  shop_id:String,
  h:Int,
  uv:Long,
  dt:Date
)
