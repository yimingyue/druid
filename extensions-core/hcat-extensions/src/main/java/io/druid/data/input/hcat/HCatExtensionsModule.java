package io.druid.data.input.hcat;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.data.input.HCatHadoopInputRowParser;
import io.druid.initialization.DruidModule;

import java.util.Arrays;
import java.util.List;

public class HCatExtensionsModule implements DruidModule {
    @Override
    public List<? extends Module> getJacksonModules() {
        return Arrays.asList(
                new SimpleModule("HCatInputRowParserModule")
                .registerSubtypes(
                        new NamedType(HCatHadoopInputRowParser.class, "hcat_hadoop")
                )
        );
    }

    @Override
    public void configure(Binder binder) {

    }
}
