package com.example.coupon;

import com.example.coupon.exception.CouponErrorCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class CouponControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    @DisplayName("userId가 없거나 본문을 읽을 수 없으면 공통 응답 포맷으로 400을 낸다")
    void 잘못된_요청_본문은_400() throws Exception {
        for (String body : List.of("", "{}", "{\"userId\":null}", "{\"userId\":\"abc\"}")) {
            mockMvc.perform(post("/api/coupons/1/issue").contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.success").value(false))
                    .andExpect(jsonPath("$.message").value(CouponErrorCode.INVALID_REQUEST.getMessage()));
        }
    }
}
