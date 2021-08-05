/*
 * Copyright 2021.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.k8s.features

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import io.fabric8.kubernetes.api.model.{HasMetadata, HostAlias, ObjectMeta, PodBuilder, Quantity, QuantityBuilder}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, KubernetesRoleSpecificConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._

private[spark] class HostAliasesFeatureStep(
    kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {

  private val conf = kubernetesConf.sparkConf

  override def configurePod(pod: SparkPod): SparkPod = {
    val hostAliases = conf.get(KUBERNETES_POD_HOSTALIASES).getOrElse("")
    val k8sPodBuilder = new PodBuilder(pod.pod)
    
    val aliasesList = hostAliases.split(",")
    for (aliases <- aliasesList) {
      val hostToIP = aliases.split(":")
      if (hostToIP.length == 2) {
        val hostList = List(hostToIP(0))
        val ip = hostToIP(1)
        k8sPodBuilder.editSpec()
          .addToHostAliases(new HostAlias(hostList.asJava, ip))
        .endSpec()
      }
    }

    val k8sPod = k8sPodBuilder.build()
    SparkPod(k8sPod, pod.container)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
