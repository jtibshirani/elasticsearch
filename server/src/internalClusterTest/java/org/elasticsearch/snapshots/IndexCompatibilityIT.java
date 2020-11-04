/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.search.SearchHit;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

public class IndexCompatibilityIT extends AbstractSnapshotIntegTestCase {

    public void testRestore60Snapshot() throws Exception {
        String version = "6.0";
        Path repoPath = randomRepoPath();
        String repoName = "test-repo";
        createRepository(repoName, "fs", repoPath);
        extractArchivedRepoToPath(version, repoPath);

        RepositoryData repositoryData = getRepositoryData(repoName);
        Collection<SnapshotId> existingSnapshots = repositoryData.getSnapshotIds();
        assertThat(existingSnapshots, Matchers.hasSize(1));

        String snapshotName = existingSnapshots.iterator().next().getName();
        RestoreInfo restoreInfo = client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName)
            .setRestoreGlobalState(false).setWaitForCompletion(true).get().getRestoreInfo();
        assertEquals(0, restoreInfo.failedShards());

        String indexName = "index";
        FieldCapabilitiesResponse fieldCapsResponse = client().prepareFieldCaps(indexName).setFields("*").get();
        Map<String, Map<String, FieldCapabilities>> fieldCaps = fieldCapsResponse.get();
        assertTrue(fieldCaps.size() > 5);

        SearchResponse searchResponse = client().prepareSearch(indexName).get();
        assertEquals(1, searchResponse.getHits().getHits().length);
        SearchHit hit = searchResponse.getHits().getAt(0);
        assertNotNull(hit.getSourceAsMap());

        searchResponse = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.nestedQuery("root", QueryBuilders.termQuery("root.name", "julie"), ScoreMode.None))
            .get();
        assertEquals(1, searchResponse.getHits().getHits().length);
    }

    private static void extractArchivedRepoToPath(String name, Path path) throws IOException {
        try (InputStream in = IndexCompatibilityIT.class.getResourceAsStream(name + "-repository.zip")) {
            TestUtil.unzip(in, path);
        }
    }
}
