package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.hcat.HCatRecordAsMap;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.joda.time.DateTime;

import java.util.List;

public class HCatHadoopInputRowParser implements InputRowParser<HCatRecord> {
    private final ParseSpec parseSpec;
    private final HCatSchema schema;
    private final List<String> dimensions;

    @JsonCreator
    public HCatHadoopInputRowParser(
            @JsonProperty("parseSpec") ParseSpec parseSpec,
            @JsonProperty("schema") HCatSchema schema
    ) {
        this.parseSpec = parseSpec;
        this.dimensions = parseSpec.getDimensionsSpec().getDimensions();
        this.schema = schema;
    }

    @Override
    public InputRow parse(HCatRecord record) {
        return parseHCatRecord(record, schema, parseSpec, dimensions);
    }

    protected static InputRow parseHCatRecord(
            HCatRecord record, HCatSchema schema, ParseSpec parseSpec, List<String> dimensions) {
        HCatRecordAsMap hCatRecordAsMap = new HCatRecordAsMap(record, schema);
        TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
        DateTime dateTime = timestampSpec.extractTimestamp(hCatRecordAsMap);
        return new MapBasedInputRow(dateTime, dimensions, hCatRecordAsMap);
    }


    @Override
    public ParseSpec getParseSpec() {
        return parseSpec;
    }

    @Override
    public InputRowParser withParseSpec(ParseSpec parseSpec) {
        return new HCatHadoopInputRowParser(parseSpec, schema);
    }
}

