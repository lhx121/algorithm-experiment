
import org.graphstream.graph.implementations._
import org.graphstream.ui.swingViewer.View;
import org.graphstream.ui.swingViewer.Viewer;
import org.graphstream.ui.swingViewer.ViewerPipe;
import scala.io.Source
/*TODO
a.需要增加可选择算法种类，如无监督检测算法，LPA改进算法等，  【SECOND】
b.输出的欺诈报告，易宝这边会更新需要输出的字段，定义含义及计算逻辑  【等其他完成后沟通】
c.需要输出一份有权值的灰名单list用于效果测试   【FIRST】  BMLPA
d.欺诈网络中各个节点需要输出汇总信息     【SECOND】
关联规则，特征：一个用户使用过的ip数，使用过的终端数等
*/


/**
  * Created by think on 2017/4/28.
  */
object graphDisplay {
  //############文件读取
  def main(args:Array[String]) {
    val graphStream:MultiGraph = new MultiGraph("GraphStream")
   // val graphStream:SingleGraph = new SingleGraph("GraphStream")
    // 设置graphStream全局属性. Set up the visual attributes for graph visualization
    graphStream.addAttribute("ui.stylesheet","url(./style/stylesheet.css)")
    graphStream.addAttribute("ui.quality")
    graphStream.addAttribute("ui.antialias")

    val verticesFile = Source.fromFile("C:\\Users\\think\\Desktop\\vertices.txt")
    for (line <- verticesFile.getLines) {
      val vertices=line.stripPrefix("(").stripSuffix(")").split(",")
      val node = graphStream.addNode(vertices(0).toString).asInstanceOf[MultiNode]
      node.addAttribute("ui.label",vertices(0).toString  +"\n"+vertices(1).toString);
      if (vertices(0).toString.length()>5 && vertices(0).toString.substring(0,5)=="10012")
        node.addAttribute("ui.class","con");
    }

    val edgesFile = Source.fromFile("C:\\Users\\think\\Desktop\\edges.txt")
    for (line <- edgesFile.getLines) {
      val edges=line.stripPrefix("Edge(").stripSuffix(")").split(",")
      val edge = graphStream.addEdge(edges(0).toString ++ edges(1).toString,
        edges(0).toString, edges(1).toString,
        true).
        asInstanceOf[AbstractEdge]
    }

    val viewer=graphStream.display();
    val view = viewer.getDefaultView();
    viewer.setCloseFramePolicy(Viewer.CloseFramePolicy.HIDE_ONLY);
    val fromViewer = viewer.newViewerPipe();

   // fromViewer.addSink(graphStream);



   //view.getCamera().setViewPercent(0.6);

  }


}
