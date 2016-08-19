// Copyright 2012,2013,2015,2016 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package roar.api.meta

import javax.xml.bind.annotation.{XmlElement, XmlRootElement}

import org.junit.{Assert, Test}
import roar.api.meta.ResourceDefinition.ResourceDynamicType
import roar.protocol.generated.RoarProtos.SearchRequest
import stark.utils.services.XmlLoader

/**
 *
 * @author jcai
 */
class ResourceDefinitionParserTest {
  @Test
  def parse() {
    val fromRd = new ResourceDefinition
    val property = new ResourceDefinition.ResourceTraitProperty
    property.name = "xm"
    property.traitProperty = "selfXM"
    fromRd.dynamicType = new ResourceDynamicType
    fromRd.dynamicType.descFormat = "test"
//    fromRd.addDynamicProperty(property)
    println(XmlLoader.toXml(fromRd))

    val rd = XmlLoader.parseXML[ResourceDefinition](getClass.getResourceAsStream("/test_res.xml"), None)
    Assert.assertEquals("czrk", rd.name)
    Assert.assertEquals("常住人口", rd.cnName)


    Assert.assertNotNull(rd.index)

    Assert.assertNotNull(rd.search)

    Assert.assertEquals(6, rd.properties.size())
    val pro = rd.properties.get(0)
    Assert.assertEquals("xm", pro.name)
    Assert.assertEquals(QueryType.String, pro.queryType)

    Assert.assertEquals(IndexType.Text, pro.indexType)

    Assert.assertEquals(5, rd.dynamicType.properties.size)
    val dPro = rd.dynamicType.properties.get(0)
    Assert.assertEquals("xm", dPro.name)
    Assert.assertEquals("xm", dPro.traitProperty)

    Assert.assertEquals(1, rd.relations.size)
    val rel = rd.relations.get(0)
    Assert.assertEquals("th", rel.name)
    Assert.assertEquals(1, rel.properties.size)
    Assert.assertEquals("sfzh", rel.properties.get(0).name)
  }
}
@XmlRootElement(name="test")
//@XmlAccessorType(XmlAccessType.FIELD)
class TestObj{
  @XmlElement(name="request")
  var request:SearchRequest = _
}
