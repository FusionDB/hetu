package org.apache.hadoop.hetu.photon.helpers;

import org.apache.hadoop.hetu.photon.meta.schema.Schema;

/**
 * Created by xiliu on 12/31/21
 */
public class InsertIgnore extends Operation {
    InsertIgnore(Schema schema) {
        super(schema);
    }

    @Override
    ChangeType getChangeType() {
        return ChangeType.INSERT_IGNORE;
    }
}
