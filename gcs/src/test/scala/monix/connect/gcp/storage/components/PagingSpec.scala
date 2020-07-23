package monix.connect.gcp.storage.components

import com.google.api.gax.paging.Page
import monix.eval.Task
import org.mockito.IdiomaticMockito
import org.mockito.Mockito.{times, verify, never}
import org.mockito.MockitoSugar.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import monix.execution.Scheduler.Implicits.global

import scala.jdk.CollectionConverters._

class PagingSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers with Paging {

  s"Paging" should {

    "download a blob" in {
      //given
      val page0 = mock[Page[String]]
      val page1 = mock[Page[String]]
      val page2 = mock[Page[String]]

      when(page0.hasNextPage).thenReturn(true)
      when(page0.getNextPage).thenReturn(page1)
      when(page0.iterateAll()).thenReturn(List("a", "b").asJava)

      when(page1.hasNextPage).thenReturn(true)
      when(page1.getNextPage).thenReturn(page2)
      when(page1.iterateAll()).thenReturn(List("c").asJava)

      when(page2.hasNextPage).thenReturn(false)
      when(page2.getNextPage).thenReturn(null)
      when(page2.iterateAll()).thenReturn(List("d").asJava)


      //when
      val t = walk(Task(page0)).toListL
      val pages = t.runSyncUnsafe()

      //then
      verify(page0, times(2)).hasNextPage
      verify(page1, times(2)).hasNextPage
      verify(page2, times(2)).hasNextPage
      verify(page0, times(1)).getNextPage
      verify(page1, times(1)).getNextPage
      verify(page2, never()).getNextPage
      pages.size shouldBe 4
    }
  }
}
