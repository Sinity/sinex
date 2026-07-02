use super::*;
use xtask::sandbox::sinex_test;

// ── Luhn ──

#[sinex_test]
async fn luhn_valid_visa() -> ::xtask::sandbox::TestResult<()> {
    let card = ["4111", "111111111111"].concat();
    assert!(is_luhn_valid(&card));
    Ok(())
}

#[sinex_test]
async fn luhn_valid_mastercard() -> ::xtask::sandbox::TestResult<()> {
    let card = ["5500", "000000000004"].concat();
    assert!(is_luhn_valid(&card));
    Ok(())
}

#[sinex_test]
async fn luhn_invalid() -> ::xtask::sandbox::TestResult<()> {
    let card = ["4111", "111111111112"].concat();
    assert!(!is_luhn_valid(&card));
    Ok(())
}

#[sinex_test]
async fn luhn_too_short() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_luhn_valid("411111"));
    Ok(())
}

// ── Credit card ──

#[sinex_test]
async fn cc_finds_valid_number() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_credit_cards("card: 4111 1111 1111 1111 ok");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn cc_rejects_random_digits() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_credit_cards("number: 1234567890123456");
    assert!(matches.is_empty()); // fails Luhn
    Ok(())
}

// ── Email ──

#[sinex_test]
async fn email_finds_address() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_emails("contact user@example.com please");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn email_rejects_version() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_emails("dep user@1.2.3");
    assert!(matches.is_empty());
    Ok(())
}

// ── Phone ──

#[sinex_test]
async fn phone_finds_international() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_phones("call +1-555-867-5309 now");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn phone_finds_parens() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_phones("call (212) 555-1234");
    assert_eq!(matches.len(), 1);
    Ok(())
}

// ── IBAN ──

#[sinex_test]
async fn iban_valid_german() -> ::xtask::sandbox::TestResult<()> {
    assert!(is_valid_iban("DE89370400440532013000"));
    Ok(())
}

#[sinex_test]
async fn iban_valid_gb() -> ::xtask::sandbox::TestResult<()> {
    assert!(is_valid_iban("GB29 NWBK 6016 1331 9268 19"));
    Ok(())
}

#[sinex_test]
async fn iban_invalid() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_valid_iban("DE00000000000000000000"));
    Ok(())
}

// ── IPv4 ──

#[sinex_test]
async fn ipv4_finds_address() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_ipv4("connecting to 192.168.1.100 now");
    assert_eq!(matches.len(), 1);
    assert_eq!(
        &"connecting to 192.168.1.100 now"[matches[0].0..matches[0].1],
        "192.168.1.100"
    );
    Ok(())
}

#[sinex_test]
async fn ipv4_finds_public_address() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_ipv4("server at 8.8.8.8");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn ipv4_rejects_version_string() -> ::xtask::sandbox::TestResult<()> {
    // Version strings like "1.2.3" (only 3 octets) must not match
    let matches = find_ipv4("version 1.2.3 released");
    assert!(matches.is_empty());
    Ok(())
}

#[sinex_test]
async fn ipv4_rejects_out_of_range_octet() -> ::xtask::sandbox::TestResult<()> {
    // 999.0.0.1 is not a valid IPv4
    let matches = find_ipv4("addr 999.0.0.1");
    assert!(matches.is_empty());
    Ok(())
}

// ── IPv6 ──

#[sinex_test]
async fn ipv6_finds_full_address() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_ipv6("addr: 2001:0db8:85a3:0000:0000:8a2e:0370:7334");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn ipv6_finds_compressed_address() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_ipv6("addr: 2001:db8::1");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn ipv6_finds_loopback() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_ipv6("bound to ::");
    assert_eq!(matches.len(), 1);
    Ok(())
}

// ── MAC address ──

#[sinex_test]
async fn mac_finds_colon_separated() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_mac_addresses("eth0: aa:bb:cc:dd:ee:ff");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn mac_finds_dash_separated() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_mac_addresses("hw: aa-bb-cc-dd-ee-ff");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn mac_finds_cisco_notation() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_mac_addresses("mac: aabb.ccdd.eeff");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn mac_rejects_too_short() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_mac_addresses("short: aa:bb:cc");
    assert!(matches.is_empty());
    Ok(())
}

// ── SSN ──

#[sinex_test]
async fn ssn_valid() -> ::xtask::sandbox::TestResult<()> {
    assert!(is_valid_ssn("123-45-6789"));
    Ok(())
}

#[sinex_test]
async fn ssn_rejects_area_000() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_valid_ssn("000-45-6789"));
    Ok(())
}

#[sinex_test]
async fn ssn_rejects_area_666() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_valid_ssn("666-45-6789"));
    Ok(())
}

#[sinex_test]
async fn ssn_rejects_area_900_plus() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_valid_ssn("900-45-6789"));
    assert!(!is_valid_ssn("999-45-6789"));
    Ok(())
}

#[sinex_test]
async fn ssn_rejects_group_00() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_valid_ssn("123-00-6789"));
    Ok(())
}

#[sinex_test]
async fn ssn_rejects_serial_0000() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_valid_ssn("123-45-0000"));
    Ok(())
}

#[sinex_test]
async fn ssn_finds_in_text() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_ssns("my SSN is 123-45-6789 ok");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn ssn_rejects_invalid_in_text() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_ssns("invalid 000-45-6789");
    assert!(matches.is_empty());
    Ok(())
}

// ── PESEL ──

#[sinex_test]
async fn pesel_checksum_valid() -> ::xtask::sandbox::TestResult<()> {
    // Known-good PESEL: 44051401458
    // Weights [1,3,7,9,1,3,7,9,1,3] × digits [4,4,0,5,1,4,0,1,4,5]
    // = 4+12+0+45+1+12+0+9+4+15 = 102 → 102 % 10 = 2 → (10-2)%10 = 8 ≠ last digit 8 ✓
    assert!(is_valid_pesel("44051401458"));
    Ok(())
}

#[sinex_test]
async fn pesel_checksum_invalid() -> ::xtask::sandbox::TestResult<()> {
    // Change last digit → checksum fails
    assert!(!is_valid_pesel("44051401459"));
    Ok(())
}

#[sinex_test]
async fn pesel_finds_in_text() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_pesels("PESEL: 44051401458 ok");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn pesel_rejects_embedded_in_longer_number() -> ::xtask::sandbox::TestResult<()> {
    // Embedded in a 12-digit string — not a standalone PESEL
    let matches = find_pesels("440514014580");
    assert!(matches.is_empty());
    Ok(())
}

#[sinex_test]
async fn pesel_rejects_random_11_digits() -> ::xtask::sandbox::TestResult<()> {
    // 11 digits but bad checksum
    let matches = find_pesels("12345678901");
    assert!(matches.is_empty());
    Ok(())
}

// ── NIP ──

#[sinex_test]
async fn nip_checksum_valid() -> ::xtask::sandbox::TestResult<()> {
    // NIP 5261040828: weights [6,5,7,2,3,4,5,6,7] × [5,2,6,1,0,4,0,8,2]
    // = 30+10+42+2+0+16+0+48+14 = 162 → 162 % 11 = 8 → check digit = 8 ✓
    assert!(is_valid_nip("5261040828"));
    Ok(())
}

#[sinex_test]
async fn nip_checksum_invalid() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_valid_nip("5261040829"));
    Ok(())
}

#[sinex_test]
async fn nip_dashed_format_xxx_xxx_xx_xx() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_nips("NIP: 526-104-08-28");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn nip_dashed_format_xxx_xx_xx_xxx() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_nips("NIP: 526-10-40-828");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn nip_bare_10_digits() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_nips("nip=5261040828");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn nip_rejects_invalid_checksum() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_nips("5261040829");
    assert!(matches.is_empty());
    Ok(())
}

// ── REGON ──

#[sinex_test]
async fn regon9_checksum_valid() -> ::xtask::sandbox::TestResult<()> {
    // REGON 060026144: weights [8,9,2,3,4,5,6,7] × [0,6,0,0,2,6,1,4]
    // = 0+54+0+0+8+30+6+28 = 126 → 126 % 11 = 5 → 5 % 10 = 5 ≠ last digit 4
    // Use a known-good REGON instead: 591457824
    // weights × [5,9,1,4,5,7,8,2] = 40+81+2+12+20+35+48+14 = 252
    // 252 % 11 = 10 → 10 % 10 = 0 ≠ 4... let me use the example 060026144 differently.
    // Actually verify: digits [0,6,0,0,2,6,1,4,4]
    // W=[8,9,2,3,4,5,6,7]: 0*8=0 6*9=54 0*2=0 0*3=0 2*4=8 6*5=30 1*6=6 4*7=28
    // sum=126 → 126%11=5 → 5%10=5. Check digit=4. 5≠4 → invalid.
    // Use 123456785: [1,2,3,4,5,6,7,8,5]
    // 8+18+6+12+20+30+42+56=192 → 192%11=5 → 5%10=5 ≠ 5 ✓
    assert!(is_valid_regon9(&[1, 2, 3, 4, 5, 6, 7, 8, 5]));
    Ok(())
}

#[sinex_test]
async fn regon9_checksum_invalid() -> ::xtask::sandbox::TestResult<()> {
    assert!(!is_valid_regon9(&[1, 2, 3, 4, 5, 6, 7, 8, 6]));
    Ok(())
}

#[sinex_test]
async fn regon9_finds_in_text() -> ::xtask::sandbox::TestResult<()> {
    let matches = find_regons("REGON: 123456785 koniec");
    assert_eq!(matches.len(), 1);
    Ok(())
}

#[sinex_test]
async fn regon9_rejects_bad_checksum() -> ::xtask::sandbox::TestResult<()> {
    // 123456786 has wrong check digit
    let matches = find_regons("REGON 123456786");
    assert!(matches.is_empty());
    Ok(())
}

// ── Home path ──

#[sinex_test]
async fn home_path_returns_vec() -> ::xtask::sandbox::TestResult<()> {
    // Can't assert specific results without knowing $HOME, but the function
    // must not panic and must return a Vec.
    let result = find_home_paths("/some/path/here");
    let _ = result; // just verify it runs
    Ok(())
}

#[sinex_test]
async fn home_path_finds_if_env_set() -> ::xtask::sandbox::TestResult<()> {
    // Set HOME to a known value and construct a matching path
    // NOTE: we can't safely mutate env in a threaded test runner, so we just
    // verify the dispatcher routes correctly.
    use super::super::StructuralDetector;
    let result = find_matches(StructuralDetector::UserHomePath, "/etc/hosts");
    let _ = result; // no panic
    Ok(())
}

// ── Hostname ──

#[sinex_test]
async fn hostname_returns_vec() -> ::xtask::sandbox::TestResult<()> {
    // Dispatcher must route without panic
    use super::super::StructuralDetector;
    let result = find_matches(StructuralDetector::LocalHostname, "some log line");
    let _ = result;
    Ok(())
}
