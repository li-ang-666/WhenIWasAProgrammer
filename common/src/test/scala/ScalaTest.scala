import org.junit.Test

class ScalaTest {
  @Test
  def test(): Unit = {
    (151 to 200).foreach(e=>{
      println(s"alter table dwd_hcm_salary_calculate_result_extend_unique add column number_column_${e} decimal(27,9);\n\n")
    })

    (61 to 100).foreach(e=>{
      println(s"alter table dwd_hcm_salary_calculate_result_extend_unique add column text_column_${e} text;\n\n")
    })
  }
}
